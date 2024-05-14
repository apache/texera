import pandas
import pytest
import rpy2.rinterface_lib.embedded
from core.models import Tuple, Table
from core.models.RTableExecutor import RTableSourceExecutor, RTableExecutor


class TestRTableExecutor:
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
    def udf_executor_simple(self):
        return """
        function(input, port) {
            return (input)
        }
        """

    @pytest.fixture
    def udf_executor_simple_extract_row(self):
        return """
        function(input, port) {
            bob_row <- input[input$Name == "Bob", ]
            return (bob_row)
        }
        """

    @pytest.fixture
    def udf_executor_simple_update_row(self):
        return """
        function(input, port) {
            input[input$Name == "Bob", "Age"] <- 18
            return (input)
        }
        """

    @pytest.fixture
    def udf_executor_simple_add_row(self):
        return """
        function(input, port) {
            new_row <- list(Name = "Test", Age = 0, City = "Irvine")
            new_df <- rbind(input, new_row)
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
    def udf_executor_df_like_type(self):
        return """
        function(input, port) {
            return (input)
        }
        """

    @pytest.fixture
    def udf_executor_df_like_type_add_row(self):
        return """
        function(input, port) {
            # Adding a new row
            new_row <- c(4, 5, 6)
            input <- rbind(input, new_row)

            return (input)
        }
        """

    @pytest.fixture
    def udf_executor_df_like_type_add_col(self):
        return """
        function(input, port) {
            # Adding a new col
            new_col <- c("AAA", "BBB")
            input <- cbind(input, new_col)

            return (input)
        }
        """

    def test_source_operator_simple(self, source_executor_simple, target_tuples_simple):
        source_operator = RTableSourceExecutor(source_executor_simple)
        output = source_operator.produce()

        tuples = [tup for tup in output]
        assert len(tuples) == 3

        for idx, v in enumerate(tuples):
            assert v == target_tuples_simple[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_simple)

    def test_udf_operator_simple(
            self, source_executor_simple, udf_executor_simple, target_tuples_simple
    ):
        source_operator = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_operator.produce()])

        udf_operator = RTableExecutor(udf_executor_simple)
        output = udf_operator.process_table(input_tbl, 0)

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
        source_operator = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_operator.produce()])

        udf_operator = RTableExecutor(udf_executor_simple_extract_row)
        output = udf_operator.process_table(input_tbl, 0)

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
        source_operator = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_operator.produce()])

        udf_operator = RTableExecutor(udf_executor_simple_update_row)
        output = udf_operator.process_table(input_tbl, 0)

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
            self,
            source_executor_simple,
            udf_executor_simple_add_row,
            target_tuples_simple
    ):
        source_operator = RTableSourceExecutor(source_executor_simple)
        input_tbl = Table([tup for tup in source_operator.produce()])

        udf_operator = RTableExecutor(udf_executor_simple_add_row)
        output = udf_operator.process_table(input_tbl, 0)

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

    def test_source_operator_fail(self, source_executor_df_fail):
        source_operator = RTableSourceExecutor(source_executor_df_fail)
        with pytest.raises(rpy2.rinterface_lib.embedded.RRuntimeError) as _:
            output = source_operator.produce()
            output = [out for out in output]

    def test_source_executor_df_like_type(
            self, source_executor_df_like_type, target_tuples_like_type
    ):
        source_operator = RTableSourceExecutor(source_executor_df_like_type)
        output = source_operator.produce()

        tuples = [tup for tup in output]
        assert len(tuples) == 2

        for idx, v in enumerate(tuples):
            assert v == target_tuples_like_type[idx]

        output_tbl = Table(tuples)
        assert output_tbl == Table(target_tuples_like_type)

    def test_udf_executor_df_like_type(
            self,
            source_executor_df_like_type,
            udf_executor_df_like_type,
            target_tuples_like_type,
    ):
        source_operator = RTableSourceExecutor(source_executor_df_like_type)
        input_tbl = Table([tup for tup in source_operator.produce()])

        udf_operator = RTableExecutor(udf_executor_df_like_type)
        output = udf_operator.process_table(input_tbl, 0)

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
        source_operator = RTableSourceExecutor(source_executor_df_like_type)
        input_tbl = Table([tup for tup in source_operator.produce()])

        udf_operator = RTableExecutor(udf_executor_df_like_type_add_row)
        output = udf_operator.process_table(input_tbl, 0)

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
            self,
            source_executor_df_like_type,
            udf_executor_df_like_type_add_col
    ):
        source_operator = RTableSourceExecutor(source_executor_df_like_type)
        input_tbl = Table([tup for tup in source_operator.produce()])

        udf_operator = RTableExecutor(udf_executor_df_like_type_add_col)
        output = udf_operator.process_table(input_tbl, 0)

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
