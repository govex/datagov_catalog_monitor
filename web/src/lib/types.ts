import type { DateTime } from "luxon";

// src/lib/types.ts
export interface CycleData {
    date: string;
    current_fileset: string;
    comparison_fileset: string;
    counts: {
        total_records: number;
        total_resources: number;
        organizations: Array<CycleOrganization>
    }
}

export interface CycleOrganization {
    id: string;
    name: string;
    title: string;
    type: string;
    description: string;
    image_url: string;
    created: string;
    is_organization: boolean;
    approval_status: string;
    state: string;
    catalog_count: number;
    resource_count: number;
}

export interface CatalogData {
    last_updated: DateTime;
    catalog_daily_statistics: EntryCount[];
    resource_daily_statistics: EntryCount[];
    organizations: Organization[];
}

export interface Organization {
    id: string;
    name: string;
    title: string;
    type: string;
    description: string;
    image_url: string;
    created: string;
    is_organization: boolean;
    approval_status: string;
    state: string;
    catalog_entry_counts: EntryCount[];
    resource_entry_counts: EntryCount[];
}

export interface EntryCount {
    date: DateTime;
    count: number;
}