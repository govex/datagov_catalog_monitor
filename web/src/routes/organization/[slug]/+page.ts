import type { PageLoad } from './$types';
import type { Organization } from '$lib/types';
import { loadHomePageData } from '$lib/loadOrganizations';

export const load: PageLoad = async ({ params, fetch }) => {

    const organization = await fetch("/data/organizations/" + params.slug + ".json").then(res => res.json()) as Organization;

    return {
        organization
    }

};