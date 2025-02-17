<script lang="ts">
    import { Badge } from 'svelte-5-ui-lib';
    let { count } = $props()

    function formatCount(count:number):string {
        if (count < 0) return `⬇ ${Math.abs(count)}`; // Handle negative difference
        if (count > 0) return `⬆ ${count}`; // Handle positive difference
        return ''
    }
</script>

{#if count > 0 }
<Badge color="blue">{ formatCount(count) }</Badge>
{:else if count < 0}
<Badge color="red">{ formatCount(count) }</Badge>
{/if}
