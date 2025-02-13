<script lang="ts">
    import OrganizationPanel from '$lib/components/OrganizationPanel.svelte';
    import { loadOrganizations } from '$lib/loadOrganizations';
    import { onMount } from 'svelte';

    import type { Organization, EntryCount } from '$lib/types';

    let organizations:Array<Organization> = [];

    onMount(async () => {
        organizations = (await loadOrganizations()).organizations;
    });
</script>

<h1>Organizations ({ organizations.length })</h1>

<div>

{#each organizations as organization}
    <OrganizationPanel {organization} />
{/each}

</div>
