def split_new_and_existing(
    incoming_df,
    silver_df
):
    """
    Splits incoming records into:
    - New episodes
    - Existing episodes
    """

    if silver_df is None:
        return incoming_df, None

    new_records = (
        incoming_df.join(
            silver_df.select("episode_id"),
            on="episode_id",
            how="left_anti"
        )
    )

    existing_records = (
        incoming_df.join(
            silver_df.select("episode_id"),
            on="episode_id",
            how="inner"
        )
    )

    return (
        new_records,
        existing_records
    )