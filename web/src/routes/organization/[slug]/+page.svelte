<script lang="ts">
	import type { PageProps } from './$types';
    import type { Organization, EntryCount } from '$lib/types';
    import { Avatar, Heading, Accordion, AccordionItem, Timeline, TimelineItem, Button } from 'svelte-5-ui-lib';
    import { ArrowLeftToBracketOutline } from 'flowbite-svelte-icons';


    import { Chart, Card, uiHelpers } from 'svelte-5-ui-lib';


	let { data } = $props();
    let organization: Organization = data.organization;
    let organizationJSON: String = JSON.stringify(organization, null, 2);

</script>

<div class="flex">
    <Avatar src={organization.image_url} alt={organization.title} class="mx-2 my-2" cornerStyle="rounded" size="lg" />
    <!-- <img src={organization.image_url} alt={organization.title} class="h-48 object-cover" /> -->
    <Heading tag="h1" class="text-primary-700 dark:text-primary-500">
        {organization.title}
    </Heading>
    <a href={ "https://catalog.data.gov/organization/" + organization.name } target="_blank" rel="noopener noreferrer" class="button">
        View on Data.gov
        <ArrowLeftToBracketOutline />
    </a>  
</div>

<p>{organization.description}</p>


<Timeline>
    {#each Object.entries(organization.catalog_entry_counts) as [date, count], i}
        <TimelineItem title={count.toString()} date={ date } dateFormat="full-date">
            &nbsp;
                <!-- <div class="flex-1">
                    <p class="text-sm text-gray-500 dark:text-gray-400">
                        {count}
                    </p>
                </div> -->
                <!-- {#if i > 0}
                    <div class="flex-1">
                        <p class="text-sm text-gray-500 dark:text-gray-400">
                            Change since {organization.catalog_entry_counts[i - 1].date}
                        </p>
                        <p class="text-lg text-primary-700 dark:text-primary-500">
                            {count.count - organization.catalog_entry_counts[i - 1].count}
                        </p>
                    </div>
                {/if} -->
        </TimelineItem>
    {/each}
</Timeline>

<Accordion flush>
    <AccordionItem title="Raw JSON">
        {#snippet header()}
            Click to view JSON
        {/snippet}
        <pre>
{ organizationJSON }
        </pre>        
    </AccordionItem> 
</Accordion>
