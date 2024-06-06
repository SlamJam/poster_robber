import pathlib
import argparse
import calendar
import datetime as dt

import pydantic
import pandas as pd

from .api import API, Transaction, ClientInfo


def df_store(fname, items: list[pydantic.BaseModel], key_field: str) -> pd.DataFrame:
    data = [item.model_dump(by_alias=False) for item in items]

    df = None
    if data:
        df = pd.DataFrame.from_records(data, index=key_field)

    if pathlib.Path(fname).exists():
        loaded = pd.read_feather(fname)

        if df is not None:
            df = pd.concat([df, loaded])
            df = df[~df.index.duplicated(keep="last")]
        else:
            df = loaded

    if df is None:
        raise Exception("Dataframe is empty")

    df.to_feather(fname)

    return df


def update_transactions(txs: list[Transaction]) -> pd.DataFrame:
    return df_store("transactions.feater", txs, "id")


def unload_transactions_for_date_range(
    api: API, date_from: dt.date, date_to: dt.date
) -> pd.DataFrame:
    txs = api.iter_transactions(date_from, date_to, per_page=500)
    return update_transactions(txs)


def update_clients(cls: list[ClientInfo]) -> pd.DataFrame:
    return df_store("clients.feater", cls, "id")


def unload_clients(api: API) -> pd.DataFrame:
    clients = api.get_clients()
    return update_clients(clients)


def to_dtime(d: dt.date | dt.datetime) -> dt.datetime:
    if isinstance(d, dt.datetime):
        return d
    elif isinstance(d, dt.date):
        return dt.datetime.combine(d, dt.datetime.min.time())
    else:
        raise Exception("unsupported type")


def command_ccr(
    api_key: str, unload_data: bool, start_period: dt.date, end_period: dt.date
):
    assert start_period < end_period
    previous_preiod_start = to_dtime(start_period - (end_period - start_period))

    if unload_data:
        assert api_key is not None
        api = API(api_key)

        print("Retrieve transactions")
        df_tx = unload_transactions_for_date_range(
            api, previous_preiod_start, end_period
        )

        print("Retrieve clients")
        df_cl = unload_clients(api)
    else:
        df_tx = update_transactions([])
        df_cl = update_clients([])

    # print(len(df_tx), len(df_cl))

    # отбираем клиентов, которые были на начало периода
    # tx_in_prev_period = df_tx[
    #     (df_tx["closed_at"] >= to_dtime(previous_preiod_start))
    #     & (df_tx["closed_at"] < to_dtime(start_period))
    # ]

    # кто совершил покупку в прошлый период
    # CS = df_cl[df_cl.index.isin(tx_in_prev_period["client_id"].unique())]

    # новые клиенты в предшествующем периоде
    CS = df_cl[
        (df_cl["activated_at"] >= previous_preiod_start)
        & (df_cl["activated_at"] < to_dtime(start_period))
    ]
    # print(CS)

    # отбираем транзакции, совершённые в заданные период
    tx_in_period = df_tx[
        (df_tx["closed_at"] >= to_dtime(start_period))
        & (df_tx["closed_at"] < to_dtime(end_period))
    ]

    # id клиентов, которые совершили покупку в рассматриваемом периоде
    clients_with_buy = tx_in_period["client_id"].unique()
    # print(clients_with_buy)

    # ушедшие клиенты - которые не совершили покупки в рассматриваемом периоде
    cl_left = CS[~CS.index.isin(clients_with_buy)]
    # print(cl_left)

    print(f"Period: [{start_period}, {end_period})")
    print("Clients at period start:", len(CS))
    print("Clients left:", len(cl_left))

    CN = df_cl[
        (df_cl["activated_at"] >= to_dtime(start_period))
        & (df_cl["activated_at"] < to_dtime(end_period))
    ]

    print("Clients new:", len(CN))
    print("Transactions in period:", len(tx_in_period))

    if len(CS) == 0:
        print("No client no period start. May be no data? Skip period")
        return

    CRR = (len(CS) - len(cl_left)) / len(CS)

    print(f"CRR: {CRR * 100:.2f}%")


def command_ccr_step_monthly(
    api_key: str, unload_data: bool, start_period: dt.date, end_period: dt.date
):
    start_period = start_period.replace(day=1)

    while start_period < end_period:
        current_end = start_period + dt.timedelta(
            days=calendar.monthrange(start_period.year, start_period.month)[1]
        )

        print(calendar.month_name[start_period.month], start_period.year)
        command_ccr(api_key, unload_data, start_period, current_end)
        print()

        start_period = current_end


def command_ccr_step_daily(
    api_key: str,
    unload_data: bool,
    start_period: dt.date,
    end_period: dt.date,
    step: int,
):
    while start_period < end_period:
        current_end = start_period + dt.timedelta(days=step)

        # print(f"Period: [{start_period}, {current_end})")
        command_ccr(api_key, unload_data, start_period, current_end)
        print()

        start_period = current_end


def command_db_info():
    df_tx = update_transactions([])
    print("Transactions count:", len(df_tx))
    print(
        "Transactions closed at min/max: {} / {}".format(
            df_tx["closed_at"].min(),
            df_tx["closed_at"].max(),
        ),
    )

    print()

    df_cl = update_clients([])
    print("Clients count:", len(df_cl))
    print(
        "Clients activated at min/max: {} / {}".format(
            df_cl["activated_at"].min(),
            df_cl["activated_at"].max(),
        ),
    )

def command_calendar(date: dt.date):
    calendar.prmonth(date.year, date.month)

def main():
    parser = argparse.ArgumentParser(
        prog="poster_robber",
        description="Анализ данныхз из Poster'а",
        epilog="Text at the bottom of help",
    )

    parser.add_argument("--api_key")

    subparsers = parser.add_subparsers(
        title="subcommands",
        description="valid subcommands",
        help="additional help",
        required=True,
        dest="subcommand",
    )

    parser_ccr = subparsers.add_parser("ccr")
    parser_ccr.add_argument("date_from", type=dt.date.fromisoformat)
    parser_ccr.add_argument("date_to", type=dt.date.fromisoformat)
    parser_ccr.add_argument("--unload_data", action="store_true")

    parser_ccr_step = subparsers.add_parser("ccr-step")
    parser_ccr_step.add_argument("date_from", type=dt.date.fromisoformat)
    parser_ccr_step.add_argument("date_to", type=dt.date.fromisoformat)
    parser_ccr_step.add_argument("--unload_data", action="store_true")

    group = parser_ccr_step.add_mutually_exclusive_group(required=True)
    group.add_argument("--monthly", action="store_true")
    group.add_argument("--daily", type=int)

    subparsers.add_parser("db-info")

    parser_cal = subparsers.add_parser("calendar")
    parser_cal.add_argument("date", type=dt.date.fromisoformat)

    args = parser.parse_args()
    # print(args)

    match (args.subcommand):
        case "ccr":
            command_ccr(args.api_key, args.unload_data, args.date_from, args.date_to)
        case "ccr-step":
            if args.monthly:
                command_ccr_step_monthly(
                    args.api_key, args.unload_data, args.date_from, args.date_to
                )
            elif args.daily is not None:
                command_ccr_step_daily(
                    args.api_key,
                    args.unload_data,
                    args.date_from,
                    args.date_to,
                    args.daily,
                )
            else:
                print("Something went wrong")
                exit(-2)
        case "db-info":
            command_db_info()
        case "calendar":
            command_calendar(args.date)
        case c:
            print("Unknown command:", c)
            exit(-1)


if __name__ == "__main__":
    main()
