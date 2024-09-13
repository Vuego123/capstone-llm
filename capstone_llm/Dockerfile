FROM public.ecr.aws/dataminded/spark-k8s-glue:v3.5.1-hadoop-3.3.6-v1

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/capstone_llm/
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
WORKDIR /opt/spark/capstone_llm/src/
CMD python3 -m capstonellm.tasks.clean -e notlocal -t dbt
#TODO add your project code and dependencies to the image
