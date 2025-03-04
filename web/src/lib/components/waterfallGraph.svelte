<script lang="ts">
    import type { EntryCount } from '$lib/types';
    let { entryCounts }: { entryCounts: EntryCount[] } = $props();
    // import { extent } from 'd3-array';
    import * as d3 from 'd3';

    let svg: SVGElement;

    const svgInfo: any = {
        width: 1920,
        height: 1080,
        viewBox: "0 0 1920 1080",
        margin: {
            top: 20,
            right: 150,
            bottom: 150,
            left: 20
        }
    }

    // Process data to get weekly aggregates
    const getWeeklyData = (data: EntryCount[]) => {
        // Filter to last 4 weeks
        const fourWeeksAgo = d3.timeWeek.offset(new Date(), -4);
        const filteredData = data.filter(d => d.t.toJSDate() > fourWeeksAgo);

        // Group by week
        const weeklyGroups = d3.group(filteredData, d => 
            d3.timeWeek.ceil(d.t.toJSDate()).getTime()
        );

        // Aggregate each week's data
        return Array.from(weeklyGroups, ([weekTimestamp, entries]) => {
            const weekDate = new Date(weekTimestamp);
            const lastEntry = entries[entries.length - 1];
            return {
                t: lastEntry.t, // Keep the DateTime type from the last entry
                n: lastEntry.n, // Use the last count of the week
                "↑": d3.sum(entries, d => d["↑"]), // Sum additions
                "↓": d3.sum(entries, d => d["↓"]), // Sum deletions
                Δ: d3.sum(entries, d => d["↑"]) - d3.sum(entries, d => d["↓"]), // Recalculate delta
            };
        });
    };

    // dump entryCounts to console whenever it changes
    $effect(() => {
        const weeklyData = getWeeklyData(entryCounts);
        console.log(weeklyData);

        const xScale = d3.scaleBand()
            .domain(weeklyData.map(d => d.t.toJSDate().toISOString()))
            .range([svgInfo.margin.left, svgInfo.width - svgInfo.margin.right])
            .padding(0.3);  // Increased padding for better visibility

        const yScale = d3.scaleLinear()
            .domain((() => {
                // Find the min/max considering both the count and the effects of additions/deletions
                const minWithDeletions = d3.min(weeklyData, d => d.n - d["↓"]) ?? 0;
                const maxWithAdditions = d3.max(weeklyData, d => d.n + d["↑"]) ?? 0;
                const buffer = (maxWithAdditions - minWithDeletions) * 0.1;
                return [minWithDeletions - buffer, maxWithAdditions + buffer];
            })())
            .range([svgInfo.height - svgInfo.margin.bottom, svgInfo.margin.top]);

        // Remove previous elements
        d3.select(svg).selectAll("path").remove();
        d3.select(svg).selectAll("circle").remove();

        // Create a group for each week
        const weekGroups = d3.select(svg)
            .selectAll("g.week")
            .data(weeklyData)
            .enter()
            .append("g")
            .attr("class", "week")
            .attr("transform", d => `translate(${xScale(d.t.toJSDate().toISOString())}, 0)`);

        // Add additions polygons (green)
        weekGroups.append("polygon")
            .attr("class", "additions")
            .attr("points", d => {
                const x0 = 0;
                const x1 = xScale.bandwidth();
                const y0 = yScale(d.n);
                const y1 = yScale(d.n + d["↑"]);
                return `${x0},${y0} ${x1},${y0} ${x1},${y1} ${x0},${y1}`;
            })
            .attr("fill", "#4CAF50")
            .attr("opacity", 0.7);

        // Add deletions polygons (red)
        weekGroups.append("polygon")
            .attr("class", "deletions")
            .attr("points", d => {
                const x0 = 0;
                const x1 = xScale.bandwidth();
                const y0 = yScale(d.n + d["↑"]);
                const y1 = yScale(d.n + d["↑"] + d["↓"]);
                return `${x0},${y0} ${x1},${y0} ${x1},${y1} ${x0},${y1}`;
            })
            .attr("fill", "#F44336")
            .attr("opacity", 0.7);

        // Add count points
        // weekGroups.append("circle")
        //     .attr("cx", xScale.bandwidth() / 2)
        //     .attr("cy", d => yScale(d.n))
        //     .attr("r", 4)
        //     .attr("fill", "black");

        // Add count labels
        weekGroups.append("text")
            .attr("x", xScale.bandwidth() / 2)
            .attr("y", d => yScale(d.n) - 10)
            .attr("text-anchor", "middle")
            .style("font-size", "16px")
            .text(d => d.n);

        // Update x-axis to use more readable date format
        d3.select(svg)
            .append("g")
            .attr("transform", `translate(0, ${svgInfo.height - svgInfo.margin.bottom})`)
            .call(d3.axisBottom(xScale)
                .tickFormat((isoString) => {
                    const date = new Date(isoString);
                    return d3.timeFormat("%b %d")(date); // Format as "Month Day"
                }))
            .selectAll("text")
            .style("font-size", "25px")
            .style("text-anchor", "start")
            .attr("dx", "-.8em")
            .attr("dy", ".15em")
            .attr("transform", "rotate(45)");

        // draw a vertical axis
        d3.select(svg)
            .append("g")
            .attr("transform", `translate(${ svgInfo.width - svgInfo.margin.right }, 0)`)
            .call(d3.axisRight(yScale))
            .selectAll("text")  // select all text elements in the axis
            .style("font-size", "25px");  // adjust this value to make labels larger or smaller
    });
</script>

<svg viewBox={svgInfo.viewBox} bind:this={svg}>

</svg>