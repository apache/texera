import pandas
import pytest
import rpy2.rinterface_lib.embedded
from core.models import Tuple, Table
from core.models.RTableExecutor import RTableSourceExecutor, RTableExecutor


class TestRTableExecutor:
    @pytest.fixture
    def source_executor_empty(self):
        return """
        function() {
            df <- data.frame(
                col1 = character(),
                col2 = numeric(),
                col3 = logical()
                )
            return (df)
        }
        """

    @pytest.fixture
    def source_executor_NA(self):
        # This should work with no issues
        # since you can store NA in a data.frame
        # and then convert the data.frame to Arrow Table with
        # one Tuple with value Tuple({"source_output": None})
        return """
        function() {
            return (NA)
        }
        """

    @pytest.fixture
    def udf_executor_return_NA(self):
        # This should fail since the conversion back
        # to Arrow will be impossible
        return """
        function(table, port) {
            return (NA)
        }
        """

    @pytest.fixture
    def udf_executor_simple_return(self):
        return """
        function(table, port) {
            return (table)
        }
        """

    @pytest.fixture
    def udf_executor_empty_add_row(self):
        return """
        function(table, port) {
            new_row <- data.frame(
                col1 = "TEST",
                col2 = 12.3,
                col3 = TRUE
              )
            table <- rbind(table, new_row)
            return (table)
        }
        """

    @pytest.fixture
    def source_executor_null_values(self):
        return """
        function() {
            df <- data.frame(
                col1 = character(),
                col2 = numeric(),
                col3 = logical()
                )
            df[1:3,] <- NA
            return (df)
        }
        """

    @pytest.fixture
    def udf_executor_null_values_return(self):
        return """
        function(table, port) {
            return (table)
        }
        """

    @pytest.fixture
    def udf_executor_null_values_add_row(self):
        return """
        function(table, port) {
            new_row <- data.frame(
                col1 = NA,
                col2 = NA,
                col3 = NA
              )
            table <- rbind(table, new_row)
            return (table)
        }
        """

    @pytest.fixture
    def target_tuples_null_values(self):
        tuple_1 = Tuple({"col1": None, "col2": None, "col3": None})
        tuple_2 = Tuple({"col1": None, "col2": None, "col3": None})
        tuple_3 = Tuple({"col1": None, "col2": None, "col3": None})
        return [tuple_1, tuple_2, tuple_3]

    @pytest.fixture
    def pandas_target_df_simple(self):
        data = {
            "Name": ["Alice", "Bob", "Charlie"],
            "Age": [25, 30, 35],
            "City": ["New York", "Los Angeles", "Chicago"],
        }
        df = pandas.DataFrame(data)
        return df

    @pytest.fixture
    def target_tuples_simple(self, pandas_target_df_simple):
        tuples = []
        for index, row in pandas_target_df_simple.iterrows():
            tuples.append(Tuple(row))
        return tuples

    @pytest.fixture
    def source_executor_simple(self):
        return """
        function() {
            df <- data.frame(
              Name = c("Alice", "Bob", "Charlie"),
              Age = c(25, 30, 35),
              City = c("New York", "Los Angeles", "Chicago")
              )
            return (df)
        }
        """

    @pytest.fixture
    def udf_executor_simple_extract_row(self):
        return """
        function(table, port) {
            bob_row <- table[table$Name == "Bob", ]
            return (bob_row)
        }
        """

    @pytest.fixture
    def udf_executor_simple_update_row(self):
        return """
        function(table, port) {
            table[table$Name == "Bob", "Age"] <- 18
            return (table)
        }
        """

    @pytest.fixture
    def udf_executor_simple_add_row(self):
        return """
        function(table, port) {
            new_row <- list(Name = "Test", Age = 0, City = "Irvine")
            new_df <- rbind(table, new_row)
            return (new_df)
        }
        """

    @pytest.fixture
    def source_executor_df_fail(self):
        # This Source UDF should raise a TypeError since it cannot
        # be converted into a Table-like object
        return """
        function() {
            glm_model <- glm(mpg ~ wt, data = mtcars, family = gaussian)
            return (glm_model)
        }
        """

    @pytest.fixture
    def target_tuples_like_type(self):
        tuple_1 = Tuple({"C.1": 1, "C.2": 2, "C.3": 3})
        tuple_2 = Tuple({"C.1": 11, "C.2": 12, "C.3": 13})
        return [tuple_1, tuple_2]

    @pytest.fixture
    def source_executor_df_like_type(self):
        return """
        function() {
            mdat <- matrix(c(1,2,3, 11,12,13), nrow = 2, ncol = 3, byrow = TRUE,
                dimnames = list(c("row1", "row2"),
                c("C.1", "C.2", "C.3")))
            return (mdat)
        }
        """

    @pytest.fixture
    def udf_executor_df_like_type_add_row(self):
        return """
        function(table, port) {
            # Adding a new row
            new_row <- c(4, 5, 6)
            table <- rbind(table, new_row)

            return (table)
        }
        """

    @pytest.fixture
    def udf_executor_df_like_type_add_col(self):
        return """
        function(table, port) {
            # Adding a new col
            new_col <- c("AAA", "BBB")
            table <- cbind(table, new_col)

            return (table)
        }
        """

    def test_source_executor_empty(self, source_executor_empty):
        source_executor = RTableSourceExecutor(source_executor_empty)
        output = source_executor.produce()
        tuples = [tup for tup in output]
        assert len(tuples) == 0

        output_tbl = Table(tuples)
        assert output_tbl == Table([])

    def test_source_executor_NA(self, source_executor_NA):
        source_executor = RTableSourceExecutor(source_executor_NA)
        output = source_executor.produce()
        tuples = [tup for tup in output]
        assert len(tuples) == 1

        output_tbl = Table(tuples)
        assert output_tbl == Table([Tuple({"source_output": None})])

    def test_udf_executor_return_NA_fail(
        self, source_executor_empty, udf_executor_return_NA
    ):
        source_executor = RTableSourceExecutor(source_executor_empty)
        input_tbl = Table([tup for tup in source_executor.produce()])

        with pytest.raises(rpy2.rinterface_lib.embedded.RRuntimeError) as _:
            udf_executor = RTableExecutor(udf_executor_return_NA)
            output = udf_executor.process_table(input_tbl, 0)
            tuples = [out for out in output]
            assert tuples is None

    def test_udf_executor_empty_return(
        self, source_executor_empty, udf_executor_simple_return
    ):
        source_executor = RTableSourceExecutor(source_executor_empty)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_simple_return)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        assert len(tuples) == 0

        output_tbl = Table(tuples)
        assert output_tbl == Table([])
        assert output_tbl == input_tbl

    def test_udf_executor_empty_add_row(
        self, source_executor_empty, udf_executor_empty_add_row
    ):
        source_executor = RTableSourceExecutor(source_executor_empty)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_empty_add_row)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        target_tuple = Tuple({"col1": "TEST", "col2": 12.3, "col3": True})
        assert len(tuples) == 1

        output_tbl = Table(tuples)
        assert output_tbl == Table([target_tuple])

    def test_source_executor_null_values(
        self, source_executor_null_values, target_tuples_null_values
    ):
        source_executor = RTableSourceExecutor(source_executor_null_values)
        output = source_executor.produce()
        tuples = [tup for tup in output]
        assert len(tuples) == 3

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_null_values)

    def test_udf_executor_null_values_return(
        self,
        source_executor_null_values,
        udf_executor_null_values_return,
        target_tuples_null_values,
    ):
        source_executor = RTableSourceExecutor(source_executor_null_values)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_null_values_return)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        assert len(tuples) == 3

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_null_values)

    def test_udf_executor_null_values_add_row(
        self,
        source_executor_null_values,
        udf_executor_null_values_add_row,
        target_tuples_null_values,
    ):
        source_executor = RTableSourceExecutor(source_executor_null_values)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_null_values_add_row)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        target_tuple = Tuple({"col1": None, "col2": None, "col3": None})
        assert len(tuples) == 4
        assert tuples[3] == target_tuple

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_null_values + [target_tuple])

    def test_source_executor_simple(self, source_executor_simple, target_tuples_simple):
        source_executor = RTableSourceExecutor(source_executor_simple)
        output = source_executor.produce()

        tuples = [tup for tup in output]
        assert len(tuples) == 3

        for idx, v in enumerate(tuples):
            assert v == target_tuples_simple[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_simple)

    def test_udf_executor_simple(
        self, source_executor_simple, udf_executor_simple_return, target_tuples_simple
    ):
        source_executor = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_simple_return)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        assert len(tuples) == 3

        for idx, v in enumerate(tuples):
            assert v == target_tuples_simple[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_simple)
        assert output_tbl == input_tbl

    def test_udf_executor_simple_extract_row(
        self,
        source_executor_simple,
        udf_executor_simple_extract_row,
        target_tuples_simple,
    ):
        source_executor = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_simple_extract_row)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        target_tuple = Tuple({"Name": "Bob", "Age": 30, "City": "Los Angeles"})
        assert len(tuples) == 1
        assert tuples[0] == target_tuple

        output_tbl = Table(tuples)
        assert output_tbl == Table([target_tuple])

    def test_udf_executor_simple_update_row(
        self,
        source_executor_simple,
        udf_executor_simple_update_row,
        target_tuples_simple,
    ):
        source_executor = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_simple_update_row)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        target_tuple = Tuple({"Name": "Bob", "Age": 18, "City": "Los Angeles"})
        assert len(tuples) == 3

        for idx, v in enumerate(tuples):
            if idx == 1:
                assert v == target_tuple
            else:
                assert v == target_tuples_simple[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(
            [target_tuples_simple[0], target_tuple, target_tuples_simple[2]]
        )

    def test_udf_executor_simple_add_row(
        self, source_executor_simple, udf_executor_simple_add_row, target_tuples_simple
    ):
        source_executor = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_simple_add_row)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        target_tuple = Tuple({"Name": "Test", "Age": 0, "City": "Irvine"})
        assert len(tuples) == 4

        for idx, v in enumerate(tuples):
            if idx == len(tuples) - 1:
                assert v == target_tuple
            else:
                assert v == target_tuples_simple[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(
            [tup for tup in target_tuples_simple] + [target_tuple]
        )

    def test_source_executor_fail(self, source_executor_df_fail):
        source_executor = RTableSourceExecutor(source_executor_df_fail)
        with pytest.raises(rpy2.rinterface_lib.embedded.RRuntimeError) as _:
            output = source_executor.produce()
            output = [out for out in output]

    def test_source_executor_df_like_type(
        self, source_executor_df_like_type, target_tuples_like_type
    ):
        source_executor = RTableSourceExecutor(source_executor_df_like_type)
        output = source_executor.produce()

        tuples = [tup for tup in output]
        assert len(tuples) == 2

        for idx, v in enumerate(tuples):
            assert v == target_tuples_like_type[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_like_type)

    def test_udf_executor_df_like_type(
        self,
        source_executor_df_like_type,
        udf_executor_simple_return,
        target_tuples_like_type,
    ):
        source_executor = RTableSourceExecutor(source_executor_df_like_type)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_simple_return)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        assert len(tuples) == 2

        for idx, v in enumerate(tuples):
            assert v == target_tuples_like_type[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_like_type)
        assert output_tbl == input_tbl

    def test_udf_executor_df_like_type_add_row(
        self,
        source_executor_df_like_type,
        udf_executor_df_like_type_add_row,
        target_tuples_like_type,
    ):
        source_executor = RTableSourceExecutor(source_executor_df_like_type)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_df_like_type_add_row)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        target_tuple = Tuple({"C.1": 4, "C.2": 5, "C.3": 6})
        assert len(tuples) == 3

        for idx, v in enumerate(tuples):
            if idx == len(tuples) - 1:
                assert v == target_tuple
            else:
                assert v == target_tuples_like_type[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_like_type + [target_tuple])

    def test_udf_executor_df_like_type_add_col(
        self, source_executor_df_like_type, udf_executor_df_like_type_add_col
    ):
        source_executor = RTableSourceExecutor(source_executor_df_like_type)
        input_tbl = Table([tup for tup in source_executor.produce()])

        udf_executor = RTableExecutor(udf_executor_df_like_type_add_col)
        output = udf_executor.process_table(input_tbl, 0)

        tuples = [tup for tup in output]
        target_tuples = [
            Tuple({"C.1": 1, "C.2": 2, "C.3": 3, "new_col": "AAA"}),
            Tuple({"C.1": 11, "C.2": 12, "C.3": 13, "new_col": "BBB"}),
        ]

        assert len(tuples) == 2
        for idx, v in enumerate(tuples):
            assert v == target_tuples[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples)
