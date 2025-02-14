<script lang="ts">
    import { Card, Heading, Secondary, Badge } from 'svelte-5-ui-lib';
    import type { Organization, EntryCount } from '$lib/types';
    import { calculateEntryCountDelta } from '$lib/calculateEntryCountDelta';
    import { formatDelta } from '$lib/formatDelta';
    import CountsGraph from './CountsGraph.svelte';
    export let organization: Organization;
</script>

<Card color="secondary">
    <Heading tag="h2" class="text-primary-700 dark:text-primary-500">{organization.title}</Heading>
    <Secondary>
        Datasets: { organization.catalog_entry_counts[organization.catalog_entry_counts.length - 1].count}     
        <Badge border color="blue">{ formatDelta(calculateEntryCountDelta(organization.catalog_entry_counts)) }&nbsp;</Badge>
    </Secondary>
</Card>