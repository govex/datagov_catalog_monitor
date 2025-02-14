<script lang="ts">
    import { Layout } from 'svelte-5-ui-lib';
    import OrganizationCard from '$lib/components/OrganizationCard.svelte';
    import { loadOrganizations } from '$lib/loadOrganizations';
    import { onMount } from 'svelte';

    import type { Organization, EntryCount } from '$lib/types';

    let organizations:Array<Organization> = [];

    onMount(async () => {
        organizations = (await loadOrganizations()).organizations;
    });
</script>

<h1>Data.gov Agencies / Bureaus / Departments ({ organizations.length })</h1>
<p>(Changes reported are since November 19, 2024)</p>

<div class="flex">
    <Layout class="grid-cols-1 gap-6 sm:grid-cols-3">
        {#each organizations as organization}
            <OrganizationCard {organization} />
        {/each}
    </Layout>
</div>
