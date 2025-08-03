"""
Cleaned and structured version of a Spark DataFrame comparison class
""" 
import sys
from typing import Optional, List, Tuple
from pyspark.sql import DataFrame, functions as F


class DataComparison:
    def __init__(self, df_1: DataFrame, df_2: DataFrame, key: str):
        """Initialize with two DataFrames and the key column to join on."""
        self.df_1 = df_1
        self.df_2 = df_2
        self.key = key

    def sort_arrays(self, df: DataFrame) -> DataFrame:
        """Sorts arrays inside struct-type columns."""
        for column in df.dtypes:
            if "struct" in str(column[1]):
                df = df.withColumn(column[0], F.sort_array(column[0]))
        return df

    def sort_struct_array(self):
        """Sort arrays in struct-type columns for both dataframes."""
        self.df_1 = self.sort_arrays(self.df_1)
        self.df_2 = self.sort_arrays(self.df_2)

    @staticmethod
    def compare_and_update_columns(
        input_df: DataFrame,
        column_list: List[str],
        key: str,
        schema_dict: dict,
        threshold: Optional[float]
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Conditions Used:
            If the column values are equal:
            → Assign NULL to the corresponding new column.

            If the column values are not equal:
            → Assign the original value to the new column.

            How Column Equality is Determined:
            For columns with double (numeric) data type:

            If both values are not null, they are considered equal if the absolute difference between them is less than the specified threshold (if provided).

            For non-numeric (e.g., string, boolean) data types:

            If the values are not equal, retain the original value; otherwise, assign NULL.


        """
        if key in column_list:
            column_list.remove(key)

        result_col = [key]
        col_expr = []

        for column_name in column_list:
            first_col = column_name + "_first"
            second_col = column_name + "_second"

            def get_expr(col1, col2):
                if schema_dict.get(column_name) == "double" and threshold is not None:
                    return F.when(
                        (F.col(col1).isNull() & F.col(col2).isNull()) |
                        (F.col(col1) == F.col(col2)) |
                        (F.abs(F.col(col1) - F.col(col2)) < threshold),
                        None
                    ).otherwise(F.col(col1))
                else:
                    return F.when(
                        (F.col(col1).isNull() & F.col(col2).isNull()) |
                        (F.col(col1) == F.col(col2)),
                        None
                    ).otherwise(F.col(col1))

            col_expr.extend([
                get_expr(first_col, second_col).alias(first_col),
                get_expr(second_col, first_col).alias(second_col)
            ])

        mismatched_data = input_df.select(result_col + col_expr)
        columns_to_check = [col for col in mismatched_data.columns if col != key]
        mismatched_data = mismatched_data.dropna(how="all", subset=columns_to_check)

        comparison_summary = mismatched_data.agg(*[
            F.count(
                F.when(
                    (F.col(c + "_first").isNull() & F.col(c + "_second").isNotNull()) |
                    (F.col(c + "_first").isNotNull() & F.col(c + "_second").isNull()) |
                    (F.col(c + "_first") != F.col(c + "_second")),
                    c
                )
            ).alias(c) for c in column_list
        ])

        all_zeros = comparison_summary.select([F.sum(col) for col in comparison_summary.columns]).first()
        if all(value == 0 for value in all_zeros):
            print("No mismatches found for the given dataframes")
        else:
            print("There are mismatches for given dataframes. Check the comparison summary and mismatched data")

        return mismatched_data, comparison_summary

    def join_dataframes(self, key: str, threshold: Optional[float]):
        """Joins dataframes and triggers comparison."""
        column_list = [col for col in self.df_2.columns if col != key]
        schema_dict = {name: dtype for name, dtype in self.df_1.dtypes}
        rename_dict_1 = {col: f"{col}_first" for col in column_list}
        rename_dict_2 = {col: f"{col}_second" for col in column_list}

        self.df_1 = self.df_1.select([F.col(c).alias(rename_dict_1.get(c, c)) for c in self.df_1.columns])
        self.df_2 = self.df_2.select([F.col(c).alias(rename_dict_2.get(c, c)) for c in self.df_2.columns])

        joined_df = self.df_1.join(self.df_2, key, "inner")
        return self.compare_and_update_columns(joined_df, column_list, key, schema_dict, threshold)

    def key_check(self, key: str) -> bool:
        """Checks if key exists in both dataframes."""
        return key in self.df_1.columns and key in self.df_2.columns

    def validate_key(self, key: str) -> bool:
        """Validates if key column has unique values in both dataframes."""
        return (
            self.df_1.select(key).distinct().count() == self.df_1.count()
            and self.df_2.select(key).distinct().count() == self.df_2.count()
        )

    def validate_equal_number_of_records(self):
        """Checks if both dataframes have equal number of rows."""
        if self.df_1.count() != self.df_2.count():
            print("Both the dataframes have unequal row count.")
        else:
            print("Both the dataframes have same row count")

    def adding_additional_columns(self, diff_dict: dict, data_df: DataFrame, column_list: List[str]) -> DataFrame:
        """Adds missing columns from diff_dict into data_df."""
        for column_itr in column_list:
            if column_itr not in data_df.columns:
                schema_datatype = diff_dict.get(column_itr)
                data_df = data_df.withColumn(column_itr, F.lit(None).cast(schema_datatype))
        return data_df.select(list(diff_dict.keys()))

    def validate_equal_structure(self) -> bool:
        """Ensures both dataframes have matching schema, and aligns them if needed."""
        dict_1 = {field.name: field.dataType for field in self.df_1.schema.fields}
        dict_2 = {field.name: field.dataType for field in self.df_2.schema.fields}

        diff_1 = dict_2.items() - dict_1.items()
        diff_2 = dict_1.items() - dict_2.items()

        if diff_1:
            print("Schema differs (extra columns in df_2)")
            self.df_1 = self.adding_additional_columns(dict_2, self.df_1, [k for k, _ in diff_1])
        if diff_2:
            print("Schema differs (extra columns in df_1)")
            self.df_2 = self.adding_additional_columns(dict_1, self.df_2, [k for k, _ in diff_2])

        self.validate_equal_number_of_records()
        return self.validate_key(self.key)

    def coverage_drop_addition(self) -> List[Optional[Tuple[float, DataFrame, str]]]:
        """Finds dropped or added keys between dataframes."""
        coverage_list = []

        df1_keys = self.df_1.select(self.key).distinct()
        df2_keys = self.df_2.select(self.key).distinct()

        dropped_keys = df1_keys.subtract(df2_keys)
        dropped_data = self.df_1.join(dropped_keys, self.key, "inner")

        dropped_count = dropped_data.count()
        if dropped_count:
            percentage = (dropped_count / self.df_1.count()) * 100
            coverage_list.append((round(percentage, 4), dropped_data, "DROP"))
        else:
            coverage_list.append(None)

        added_keys = df2_keys.subtract(df1_keys)
        added_data = self.df_2.join(added_keys, self.key, "inner")

        added_count = added_data.count()
        if added_count:
            percentage = (added_count / self.df_1.count()) * 100
            coverage_list.append((round(percentage, 4), added_data, "ADD"))
        else:
            coverage_list.append(None)

        return coverage_list

    def process_df(self, local_test_cases: bool) -> Tuple[DataFrame, DataFrame]:
        """Filters data to common keys only."""
        common_keys = self.df_1.join(self.df_2, self.key, "inner").select(self.key).distinct()
        self.df_1 = self.df_1.join(common_keys, self.key, "inner")
        self.df_2 = self.df_2.join(common_keys, self.key, "inner")
        return self.df_1, self.df_2

    def calling_compare_dataframes(
        self, key: str, threshold: Optional[float], local_test_cases: bool
    ) -> Tuple[DataFrame, DataFrame, Optional[Tuple], Optional[Tuple]]:
        """Master function to call all checks and return mismatches and coverage info."""
        if not self.key_check(key):
            raise ValueError(f"Key '{key}' not found in both dataframes")

        coverage_list = self.coverage_drop_addition()

        if self.validate_equal_structure():
            self.df_1, self.df_2 = self.process_df(local_test_cases)
            self.sort_struct_array()
            self.df_1 = self.df_1.select(self.df_2.columns)
            count_mismatch = self.df_1.subtract(self.df_2).count()

            if any(coverage_list) or count_mismatch != 0:
                mismatched_data, comparison_summary = self.join_dataframes(key, threshold)
                return (
                    mismatched_data,
                    comparison_summary,
                    coverage_list[0] if coverage_list else [],
                    coverage_list[1] if coverage_list else []
                )

            dummy = spark.createDataFrame([], self.df_1.schema)
            return dummy, dummy, [], []
        else:
            print("Validation fails")
            sys.exit()


def compare_dataframes(
    df_1: DataFrame,
    df_2: DataFrame,
    key: str,
    threshold: Optional[float] = None,
    local_test_cases: Optional[bool] = False
) -> Tuple[DataFrame, DataFrame, Optional[Tuple], Optional[Tuple]]:
    """Wrapper function to use the DataComparison class in a single call."""
    comparator = DataComparison(df_1, df_2, key)
    return comparator.calling_compare_dataframes(key, threshold, local_test_cases)
