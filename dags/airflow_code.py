from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator


#first code
def add_values():
    x = 5 
    y = 2
    z = x+y
    return z

#second code
def concatenating_list():
    my_list1 =['a','d','x'] 
    my_list2 =['g','v','e'] 
    my_list1 .extend(my_list2)
    return my_list1

#third code
def sorting_list():
    my_list1 =['a','d','x'] 
    my_list1.sort(reverse=True)
    return my_list1

#fourth code 
def append_list():
    my_list1 =['a','d','x']
    my_list3 = [3,5,6]
    my_list1.append(my_list3)
    return my_list1

#fifth code
def copy_list():
    my_list1 =['a','d','x']
    new_list = my_list1.copy()
    new_list.reverse()
    return new_list

#DAG Configuration
airflow_assignment = DAG(
    dag_id="my_first_dag",
    description="first airflow assignment",
    tags=["assignment","python_code","first_dag"],
    default_view="graph"
)

#Task configuration
First_task = PythonOperator(
    python_callable=add_values,
    task_id="additions",
    dag=airflow_assignment,
    owner="bimmy"
)

Second_task = PythonOperator(
    python_callable=concatenating_list,
    task_id="concatenation",
    dag=airflow_assignment,
    owner="bimmy"
)

Third_task = PythonOperator(
    python_callable=sorting_list,
    task_id="sorting",
    dag=airflow_assignment,
    owner="bimmy"
)

Fourth_task = PythonOperator(
    python_callable=append_list,
    task_id="appending",
    dag=airflow_assignment,
    owner="bimmy"
)

Fifth_task = PythonOperator(
    python_callable=copy_list,
    task_id="copying",
    dag=airflow_assignment,
    owner="bimmy"
)

#setting dependencies
First_task>>Second_task>>Third_task>>Fourth_task>>Fifth_task
