# %%
# imports and initalization
import polars as pl
from pathlib import Path
import logging
from datetime import datetime
import os
import matplotlib.pyplot as plt



# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get data directory from environment variable with fallback
DATA_DIR = Path(os.getenv('DATA_CACHE_DIR', 'data'))

# Find most recent catalog folder
catalog_dir = DATA_DIR / 'data_gov_catalog_ndjson'
catalog_folders = [d for d in catalog_dir.iterdir() if d.is_dir()]
latest_folder = max(catalog_folders, key=lambda x: datetime.strptime(x.name, '%Y%m%dT%H%M%S'))

logger.info(f"Processing catalog data from {latest_folder}")

# Create tags output directory
tags_dir = DATA_DIR / 'tags'
tags_dir.mkdir(exist_ok=True)

# Use scan_ndjson to lazily read all files
lf = pl.scan_ndjson(
    latest_folder,
    schema={
        "id": pl.Utf8,
        "tags": pl.List(
            pl.Struct({
                "display_name": pl.Utf8,
                "id": pl.Utf8,
                "name": pl.Utf8,
                "state": pl.Utf8,
                "vocabulary_id": pl.Utf8
            })
        ),
        "organization": pl.Struct({
            "id": pl.Utf8,
            "name": pl.Utf8,
            "title": pl.Utf8,
            "type": pl.Utf8,
            "description": pl.Utf8,
            "image_url": pl.Utf8,
            "created": pl.Utf8,
            "is_organization": pl.Boolean,
            "approval_status": pl.Utf8,
            "state": pl.Utf8
        })
    }
)

# %%
# Generate organization x tag matrix
# First collect the raw data we need
raw_df = (
    lf
    .select([
        pl.col("organization").struct.field("name").alias("organization_name"),
        pl.col("tags")
    ])
    .collect()
)

# Then do the transformations on the eager DataFrame
org_tag_matrix = (
    raw_df
    .explode("tags")
    .with_columns([
        pl.col("tags").struct.field("name").alias("tag_name")
    ])
    .drop("tags")
    .group_by(["organization_name", "tag_name"])
    .agg(pl.len().alias("count"))
    .pivot(
        index="organization_name",
        columns="tag_name",
        values="count",
        aggregate_function="sum"
    )
)

# %%

# Save the matrix to CSV
output_file = tags_dir / f"org_tag_matrix_{latest_folder.name}.csv"
org_tag_matrix.write_csv(output_file)

# Log some basic statistics
logger.info(f"Generated organization x tag matrix with shape: {org_tag_matrix.shape}")
logger.info(f"Saved matrix to {output_file}")

# Display the first few rows and columns as a preview
print("\nPreview of the organization x tag matrix:")
print(org_tag_matrix.head().select(pl.col("organization_name"), pl.all().exclude("organization_name").head(5)))



# # %%
# # Extract and explode tags, then get unique combinations
# unique_tags = (
#     lf.select(pl.col('tags').list.explode())
#     .unnest('tags')
#     .group_by("name")  # Group by tag name
#     .agg(pl.len().alias("count")) 
#     # .filter(pl.col("count") >= 10)  # Count occurrences of each tag
#     .sort("count", descending=True)
#     .collect()
# )


# # %%
# # Step 1: Extract relevant fields and explode `tags`
# org_tags_df = (
#     lf
#     .select(
#         pl.col("organization").struct.field("name").alias("organization_name"),  # Extract organization name
#         pl.col("tags").list.explode()  # Expand tags into separate rows
#     )
#     .unnest("tags")  # Extract tag fields
#     .group_by("name", "organization_name")  # Group by tag and organization
#     .agg(pl.len().alias("count"))  # Count occurrences
#     .collect()
# )

# # %%
# # Step 2: Filter tags that appear in more than 100 catalog entries
# high_freq_tags = (
#     org_tags_df
#     .group_by("name")
#     .agg(pl.sum("count").alias("total_count"))  # Sum tag occurrences across all organizations
#     .filter(pl.col("total_count") > 100)  # Keep only tags with more than 100 occurrences
#     .select("name")  # Extract tag names
# )

# # %%
# # Step 3: Filter original dataset to only include high-frequency tags
# filtered_df = org_tags_df.join(high_freq_tags, on="name")

# # Step 4: Pivot the DataFrame to create the matrix
# org_tag_matrix = filtered_df.pivot(
#     index="organization_name",
#     columns="name",
#     values="count",
#     aggregate_function="sum"
# )

# # %%
# # Assuming tags_count_df is your Polars DataFrame
# tag_counts = unique_tags["count"].to_list()

# # Create histogram
# plt.figure(figsize=(12, 6))
# plt.hist(tag_counts, bins=20, edgecolor="black", log=True)

# plt.xlabel("Tag Count")
# plt.ylabel("Frequency")
# plt.title("Histogram of Tag Counts")
# plt.grid(axis="y", linestyle="--", alpha=0.7)

# plt.show()

# # %%
# # Save to CSV
# output_file = tags_dir / f"unique_tags_{latest_folder.name}.csv"
# unique_tags.write_csv(output_file)

# # Log statistics
# total_records = lf.select(pl.count()).collect().item()
# total_unique_tags = len(unique_tags)

# logger.info(f"Processed {total_records:,} records")
# logger.info(f"Found {total_unique_tags:,} unique tags")
# logger.info(f"Saved tags to {output_file}")

# # Display tag frequency statistics if tags have a 'name' field
# if 'name' in unique_tags.columns:
#     logger.info("\nMost common tag names:")
#     name_counts = unique_tags.select('name').value_counts()
#     print(name_counts.head(10))

