<script lang="ts">
    import type { Organization, EntryCount } from '$lib/types';
    import { calculateEntryCountDelta } from '$lib/calculateEntryCountDelta';
    import { formatDelta } from '$lib/formatDelta';
    import CountsGraph from './CountsGraph.svelte';
    export let organization: Organization;
</script>

<div class="organization">
    <h2>{organization.title}</h2>
    <p>Change since February 2: { formatDelta(calculateEntryCountDelta(organization.catalog_entry_counts)) }</p>
    <p>{organization.description}</p>
    <!-- <CountsGraph { organization.catalog_entry_counts } /> -->
    <table style="display: none"><tbody>
        {#each organization.catalog_entry_counts as entryCount:EntryCount }
            <tr>
                <td>{entryCount.date}</td>
                <td>{entryCount.count}</td>
            </tr>
        {/each}
    </tbody></table>
</div>