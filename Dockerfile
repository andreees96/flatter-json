FROM apache/beam_python3.7_sdk
USER root
WORKDIR /app
COPY main.py /app/main.py
<<<<<<< HEAD
ENV PYTHONBUFFER = 1
ENTRYPOINT ["python", "main.py"]
=======
COPY data /app/data
RUN pip install pyarrow==11.0.0
ENV PYTHONBUFFERED=1
ENTRYPOINT ["python", "main.py"]
>>>>>>> 38dc7e8 (Create solution with Python and Apache Beam for data management)
