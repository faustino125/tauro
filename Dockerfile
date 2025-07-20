FROM continuumio/miniconda3:23.10.0-1

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jdk \
    libgl1-mesa-glx \
    libxrender1 \
    libxext6 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN conda create -y -n tauro python=3.10 && \
    conda clean -ya

ENV PATH /opt/conda/envs/tauro/bin:$PATH
SHELL ["conda", "run", "-n", "tauro", "/bin/bash", "-c"]

WORKDIR /tauro

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8888 8080 4040 8000

CMD ["/bin/bash"]
