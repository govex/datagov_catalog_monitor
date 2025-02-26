import { DateTime } from "luxon";
import type { Organization, CatalogData, EntryCount } from "$lib/types";

// Function to parse a date string (Assumes format: YYYYMMDDTHHMMSS)
const parseCustomDate = (dateStr: string): Date => {
    return DateTime.fromFormat(dateStr, "yyyyMMdd'T'HHmmss", { zone: "utc" }).toJSDate();
};  

// Function to load and parse JSON data
export async function loadHomePageData(): Promise<{ catalogData: CatalogData }> {
    const response = await fetch('/data/organizations.json'); // Load from static file
    if (!response.ok) {
        throw new Error("Failed to load JSON data");
    }

    const rawData = await response.json();

    const catalogData: CatalogData = {
        last_updated: DateTime.fromISO(rawData.last_updated),
        catalog_daily_statistics: [],
        resource_daily_statistics: [],
        organizations: []
    };

    const organizations: Organization[] = []
    // Ensure proper types and coerce incoming data structure into the type model
    for (const org of Object.values(rawData.organizations) as any[]) {
        catalogData.organizations.push({
            ...org,
            catalog_entry_counts: Object.entries(org.catalog_entry_counts).map(([key, value]) => {
                return { date: DateTime.fromISO(key), count: value };
            }),
            resource_entry_counts: Object.entries(org.catalog_entry_counts).map(([key, value]) => {
                return { date: DateTime.fromISO(key), count: value };
            })
        });
    };


    // window.rawData = rawData;
    // window.catalogData = catalogData;


    return { catalogData };
}

