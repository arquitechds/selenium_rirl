FROM seleniarm/standalone-chromium:latest

USER root
RUN apt-get update && apt-get install -y python3-full

# Crea entorno virtual y lo activa
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Instala pip y librerías
#COPY requirements.txt .
#RUN pip install --upgrade pip && pip install -r requirements.txt

WORKDIR /app
COPY . /app

CMD ["python", "main.py"]
