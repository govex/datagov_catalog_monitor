<script lang="ts">
    import { Card, Heading, Secondary } from 'svelte-5-ui-lib';
    import type { Organization } from '$lib/types';
    import { calculateEntryCountDelta } from '$lib/calculateEntryCountDelta';
    import ColoredDeltaBadge from './ColoredDeltaBadge.svelte';
    export let organization: Organization;
</script>

<a href={`/organization/${organization.id}`}>
    <Card color="secondary" class="flex flex-col bg-white shadow-md rounded-lg h-full" padding="sm">
        <img src={organization.image_url} alt={organization.title} class="object-contain h-32 w-full" />
        <Heading tag="h3" class="text-primary-700 dark:text-primary-500 flex-grow">{organization.title}</Heading>
        <div class="flex flex-row justify-between content-center text-sm text-gray-500 v-full mt-5">
            <Secondary>
                Datasets: { organization.catalog_entry_counts[organization.catalog_entry_counts.length - 1].count}     
            </Secondary>
            <ColoredDeltaBadge count={ calculateEntryCountDelta(organization.catalog_entry_counts) } />
        </div>           
    </Card>
</a>
