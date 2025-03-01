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

    window.rawData = rawData;

    rawData.last_updated = DateTime.fromISO(rawData.last_updated);

    // coerce date strings to DateTime objects
    for (const ce: EntryCount of rawData.catalog_daily_statistics) {
        ce.t = DateTime.fromISO(ce.t);
    };

    for (const ce: EntryCount of rawData.resource_daily_statistics) {
        ce.t = DateTime.fromISO(ce.t);
    }


    // iterate through the dictionary of organizations and coerce the date strings to DateTime objects
    for (const org of Object.values(rawData.organizations) as any[]) {
        for (const ce: EntryCount of org.catalog_entry_counts) {
            ce.t = DateTime.fromISO(ce.t);
        }
        for (const ce: EntryCount of org.resource_entry_counts) {
            ce.t = DateTime.fromISO(ce.t);
        }
    }

    // convert organizations to an array
    rawData.organizations = Object.values(rawData.organizations) as Organization[];
    rawData.organizations.sort((a, b) => a.title.localeCompare(b.title));

    console.log(rawData);


    return { catalogData:rawData };
}

