import requests
import pandas as pd
import logging
import concurrent.futures as cf
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


def get_log_file_name() -> str:
    """Opens results txt file and assigns name appending the current time. File will keep a log of transactions"""
    ts = datetime.now()
    base_path = r".\logs\policyCancellationLogs_"
    file_name = f"{base_path}-{str(ts.hour)}{str(ts.minute)}-{str(ts.second)}.txt"
    return file_name


def success_df(
    results_success: pd.DataFrame, policy_number: str, response: str, content: bytes | Any
):
    """Appends the policyNumber and response for successfull (200 responses) cancellations to the resultsSuccess dataframe"""
    results_success = results_success._append(
        {"Success": str(policy_number), "Response": str(response), "Content": str(content)},
        ignore_index=True,
    )


def failure_df(
    results_failure: pd.DataFrame, policy_number: str, response: str, content: bytes | Any
):
    """Appends the policyNumber and response for failed (anything other than 200 responses) cancellations to the resultsFailure dataframe"""
    results_failure = results_failure._append(
        {"Failure": str(policy_number), "Response": str(response), "Content": str(content)},
        ignore_index=True,
    )


def results_csv_creation(results_success: pd.DataFrame, results_failure: pd.DataFrame):
    """Creates two csv files, one for all successful cancellations and one for failures, from those dataframes, and places them in the results folder"""
    ts = datetime.now()

    base_path = r".\logs\results\policyCancellationResults_"
    success_file_name = f"{base_path}Success_{str(ts.year)}{str(ts.month)}{str(ts.day)}-{str(ts.hour)}{str(ts.minute)}-{str(ts.second)}.csv"
    failure_file_name = f"{base_path}Failure_{str(ts.year)}{str(ts.month)}{str(ts.day)}-{str(ts.hour)}{str(ts.minute)}-{str(ts.second)}.csv"

    _ = results_success.to_csv(success_file_name, index=False)
    _ = results_failure.to_csv(failure_file_name, index=False)

    print(success_file_name)
    print(failure_file_name)


def assign_variables(in_file: str):
    """Creates pandas dataframe using the a csv spreadsheet named LDPolicyExport.csv in the same directory.
    Iterates through all rows in the sheet, calls on sendRequest()

    If the POLICYNUMBER is blank in the provided data, then it will be replaced with 99999999

    Creates two dataframes, one for successful responses and one for failures. During each iteration results are appended to the corresponding dataframe
    using successDF() and failureDF().

    After all iterations have completed resultsCSVCreation is called using the completed dataframes.
    """

    print("Begin extracting csv data.")
    results_success = pd.DataFrame({"Success": [], "Response": [], "Content": []})
    results_failure = pd.DataFrame({"Failure": [], "Response": [], "Content": []})

    extract_df = pd.read_csv(in_file, keep_default_na=False)

    req_future: list[cf.Future[requests.Response]] = []
    future_args: list[str] = []

    with cf.ThreadPoolExecutor(8) as exe:
        for index, _ in extract_df.iterrows():
            policy_number = extract_df.loc[index, "POLICYNUMBER"]
            period_start = extract_df.loc[index, "PERIODSTART"].split(r"/")
            month = period_start[0]
            day = period_start[1]
            year = period_start[2]

            logger.info("-" * 20)
            if policy_number == "":
                policy_number = str(999999999)
                print("No POLICYNUMBER in provided csv file, setting policyNumber to 99999999")
                logger.warning(
                    "No POLICYNUMBER in provided csv file, setting policyNumber to 99999999"
                )
            logger.info(
                f"{str(policy_number)}\n{str(period_start)}\n month:{str(month)} day:{str(day)} year:{str(year)}\n{str(datetime.now())}"
            )

            print("policyNumber " + str(policy_number))
            print("day " + str(day))
            print("month " + str(month))
            print("year " + str(year))
            print(datetime.now())

            req_future.append(exe.submit(send_request, policy_number, year, month, day))
            future_args.append(policy_number)

    for future, policy_number in zip(req_future, future_args):
        try:
            rawResponse = future.result()
            response = str(rawResponse)
            content = rawResponse.content
            future.__dir__
            if str(response) == "<Response [200]>":
                success_df(results_success, policy_number, response, content)
            else:
                failure_df(results_failure, policy_number, response, content)
            pass
        except Exception as e:
            print(f"Exception occurred during request {e}")
    results_csv_creation(results_success, results_failure)


def send_request(policy_number: str, year: str, month: str, day: str):
    """Using Requests module, send Post request as defined.

    policyNumber, year, month, day, and file are provided by assignVariables(), which are then inserted into the JSON body under the Payload.

    Returns response, if != to <Response [200]> the content of the response is recorded in logs"""

    print("Begin sendRequest.")
    url = ""
    year = year
    month = month
    day = day
    policy_number = policy_number

    payload = """""" % {}

    headers = {}

    response = requests.request("POST", url, data=payload, headers=headers)
    content = response.content

    print(str(response))
    print(str(response.elapsed.total_seconds()) + "\n")

    logger.info(str(response))
    if str(response) != "<Response [200]>":
        print(str(content) + "\n")
    return response


def main():
    logging.basicConfig(filename=get_log_file_name(), level=logging.INFO)
    assign_variables("LDPolicyExport.csv")
    time = datetime.now()
    logger.info("************************Run completed" + "_" + str(time))


if __name__ == "__main__":
    main()
