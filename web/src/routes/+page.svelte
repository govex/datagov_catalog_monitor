<script lang="ts">
    import { Layout } from 'svelte-5-ui-lib';
    import OrganizationCard from '$lib/components/OrganizationCard.svelte';
    import { loadHomePageData } from '$lib/loadOrganizations';
    import { onMount } from 'svelte';

    import type { CatalogData, Organization, EntryCount } from '$lib/types';

    let catalogData: CatalogData | undefined = undefined;
    let organizations: Organization[] = [];

    onMount(async () => {
        catalogData = (await loadHomePageData()).catalogData;
        console.log(catalogData);
    });
</script>

<p>Data.gov Agencies / Bureaus / Departments: { catalogData?.organizations?.length || "loading..." }</p>
<p>Changes reported are since November 19, 2024</p>
<p>Last updated: { catalogData?.last_updated.toRelative() || "loading..." }</p>

<div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-4 w-full max-w-screen-lg mx-auto">
    <!-- <Layout class="grid-cols-1 gap-6 sm:grid-cols-3"> -->
        {#each catalogData?.organizations ?? [] as organization}
            <OrganizationCard {organization} />
        {/each}
    <!-- </Layout> -->
</div>
