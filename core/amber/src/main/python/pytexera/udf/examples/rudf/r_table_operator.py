# Note: make sure R path is initialized in udf.conf and make sure that
# the following packages (in R) are installed: arrow
# --- Source Operator Examples ---
r_table_source_simple_table = """
function() {
    df <- data.frame(
      Training = c("Strength", "Stamina", "Other"),
      Pulse = c(100L, 150L, 120L),
      Duration = c(60, 30, 45)
      )
    return (df)
}
"""

r_table_source_like_objects = """
function() {
    # Works with R lists, R vectors, R matrices,
    # or anything that can converted to a data.frame

    # Matrix
    mdat <- matrix(c(1,2,3, 11,12,13), nrow = 2, ncol = 3, byrow = TRUE,
        dimnames = list(c("row1", "row2"),
        c("col1", "col2", "col3")))

    # List
    lst <- list(col1 = c(1,2), col2 = c(2,12), col3 = c(3,13))

    # Vectors
    col1_vec <- c(1,11)
    col2_vec <- c(2,12)
    col3_vec <- c(3,13)
    df_from_vec <- data.frame(col1_vec, col2_vec, col3_vec)

    return (mdat)
}
"""

# --- UDF Operator Examples ---
r_table_udf_echo_table = """
function(table, port) {
    return (table)
}
"""

r_table_udf_add_row = """
function(table, port) {
    # Assuming table is:
    # data.frame(
    #       Training = c("Strength", "Stamina", "Other"),
    #       Pulse = c(100L, 150L, 120L),
    #       Duration = c(60, 30, 45)
    # )
    new_row <- list(Training = "NEW", Pulse = 999L, Duration = 999)
    new_df <- rbind(table, new_row)
    return (new_df)
}
"""

r_table_udf_extract_row = """
function(table, port) {
    # Assuming table is:
    # data.frame(
    #       Training = c("Strength", "Stamina", "Other"),
    #       Pulse = c(100L, 150L, 120L),
    #       Duration = c(60, 30, 45)
    # )
    tuple <- table[1,]
    return (tuple)
}
"""
