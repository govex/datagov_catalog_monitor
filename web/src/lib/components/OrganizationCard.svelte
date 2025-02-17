<script lang="ts">
    import { Card, Heading, Secondary, Badge } from 'svelte-5-ui-lib';
    import type { Organization, EntryCount } from '$lib/types';
    import { calculateEntryCountDelta } from '$lib/calculateEntryCountDelta';
    import { formatDelta } from '$lib/formatDelta';
    import ColoredDeltaBadge from './ColoredDeltaBadge.svelte';
    import CountsGraph from './CountsGraph.svelte';
    export let organization: Organization;
</script>

<div class="w-full sm:w-1/2 lg:w-1/3 p-4">
    <Card color="secondary" class="h-full">
        <Heading tag="h2" class="text-primary-700 dark:text-primary-500">{organization.title}</Heading>
        <Secondary>
            Datasets: { organization.catalog_entry_counts[organization.catalog_entry_counts.length - 1].count}     
            <ColoredDeltaBadge count={ calculateEntryCountDelta(organization.catalog_entry_counts) } />
        </Secondary>
    </Card>
</div>
