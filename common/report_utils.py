from datetime import datetime, timezone

from pyspark.sql import Row


class ValidationReport:

    def __init__(
        self,
        spark,
        pipeline_name,
        job_name,
        bucket_name
    ):

        self.spark = spark

        self.pipeline_name = pipeline_name

        self.job_name = job_name

        self.bucket_name = bucket_name

        self.execution_time = datetime.now(timezone.utc)

        self.results = []

    def add_result(
            self,
            validation_name,
            status,
            severity,
            expected,
            actual,
            remarks=""
    ):

        self.results.append(

            Row(

                pipeline=self.pipeline_name,

                glue_job=self.job_name,

                execution_time=self.execution_time.isoformat(),

                validation_name=validation_name,

                status=status,

                severity=severity,

                expected=str(expected),

                actual=str(actual),

                remarks=remarks

            )

        )

    def write_report(self):

        if not self.results:
            return None

        report_df = self.spark.createDataFrame(
            self.results
        )

        year = self.execution_time.strftime("%Y")

        month = self.execution_time.strftime("%m")

        day = self.execution_time.strftime("%d")

        timestamp = self.execution_time.strftime(
            "%Y%m%d_%H%M%S"
        )

        output_path = (

            f"s3://{self.bucket_name}/"

            f"reports/"

            f"{self.pipeline_name.lower()}/"

            f"{year}/{month}/{day}/"

            f"{timestamp}/"

        )

        report_df = report_df.coalesce(1)

        try:

            report_df.write \
                .mode("overwrite") \
                .option("header", True) \
                .csv(output_path)

            return output_path


        except Exception as e:

            raise Exception(

                f"Failed to write validation report to {output_path}. Error: {str(e)}"

            ) from e