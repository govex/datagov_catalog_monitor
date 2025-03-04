<script lang="ts">
    import { scaleTime, scaleLinear } from 'd3-scale';
    import { extent, max } from 'd3-array';
    import { line, curveBasis } from 'd3-shape';
	import type { EntryCount } from '$lib/types';
	import { onMount } from 'svelte';
    
    // add a property for an array of EntryCount objects
    let entryCounts = $props<{ entryCounts: EntryCount[] }>();

	const width: number = 1920;
	const height: number = 500;

    const margin = { top: 20, right: 30, bottom: 20, left: 30 };

	let xScale = $derived(
		entryCounts && width
			? scaleTime()
					.domain(extent(entryCounts, "date"))
					.range([margin.left, width - margin.right])
			: null
	);

	let yScale = $derived(
		entryCounts && width
			? scaleLinear()
					.domain([0, max(entryCounts, "count")])
					.range([height - margin.bottom, margin.top])
			: null
	);

	let lineGenerator = $derived(
		xScale && yScale
			? line()
					.x((d:EntryCount) => xScale(d.date))
					.y((d:EntryCount) => yScale(d.count))
					.curve(curveBasis)
			: null
	);

	onMount(() => {
		console.log('CountsGraph mounted');
		console.log(entryCounts);
	});
</script>

<svg { width } { height } xmlns="http://www.w3.org/2000/svg">
	<g transform="translate(0, 100) scale(1, -1)">
		<path
			d="M 0 0 L 10 10 L 20 20 L 30 30 L 40 40 L 50 50 L 60 60 L 70 70 L 80 80 L 90 90 L 100 100"
			fill="none"
			stroke="black"
			stroke-width="1"
		/>
	</g>
</svg>
