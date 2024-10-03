# Import python packages

import snowflake.connector

from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)
import pandas as pd
import streamlit as st


def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a UI on top of a dataframe to let viewers filter columns

    Args:
        df (pd.DataFrame): Original dataframe

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    modify = st.checkbox("Add filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(str).str.contains(user_text_input)]

    return df

# Write directly to the app
st.title("Active Staff Data")
st.write(
    """When using filters, please be aware that filtering is **case-sensitive**.
    """
)

conn = snowflake.connector.connect(
        user='DEPUTYUSER',
        password='wm$4Zh62',
        account='dfzqycb-tz75056',
        warehouse='COMPUTE_WH',
        database='DEPUTY',
        schema='RAW')

cursor = conn.cursor()

query = '''
select t0.id,
concat(first_name, ' ', last_name) as full_name,
display_name,
start_date::date as start_date,
termination_date::date as termination_date,
employee_hourly_rate as pay_rate,
case
    when start_date is null then null
    when termination_date is null then datediff(day, start_date, current_timestamp())
    else datediff(day, start_date, termination_date)
end as days_employed,
value:Module as module_id,
value:TrainingDate::date as training_date,
t1.title as title,
case
    when training_date is null then datediff(day, start_date, current_timestamp())
    else datediff(day, training_date, current_timestamp())
end as days_at_level,
active,
t2.total_time as hrs_worked_fortnight,
t2.total_shifts as total_shifts_fortnight,
t2.average_shift_length as average_shift_length_fortnight
from deputy.fact.employees_fact_recent as t0
left join table(flatten(input => t0.training_array, outer => TRUE)) f 
left join deputy.fact.training_module_recent as t1
on module_id = t1.id
left join deputy.fact.hours_by_employee_recent_cycle as t2
on t0.id = t2.employee
where active
--where (title is null or title in ('Level 1', 'Level 2', 'Level 3', 'MOD'))
--qualify row_number() over (partition by t0.id order by title desc) = 1
order by t0.id;
'''

cursor.execute(query)
result = cursor.fetchall()

df = pd.DataFrame(result,
                      columns=['ID', 'full_name', 'display_name', 'start_date', 'termination_date', 'pay_rate', 'days_employed', 'module_id', 'training_date', 'title', 'days_at_level', 'active', 'hrs_worked_fortnight', 'total_shifts_fortnight', 'average_shift_length_fortnight'])

st.dataframe(filter_dataframe(df))