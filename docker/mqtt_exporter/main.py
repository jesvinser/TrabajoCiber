#!/usr/bin/env python3
""" MQTT Bridge Exporter: Lee de Broker Externo -> Métrica -> Broker Interno """

import logging
import os
import sys
import time
import paho.mqtt.client as mqtt
from prometheus_client import Counter, Gauge, start_http_server

# --- CONFIGURACIÓN ---
# Broker EXTERNO (Fuente)
EXT_BROKER = os.getenv("EXT_MQTT_ADDRESS", "broker.hivemq.com")
EXT_PORT = int(os.getenv("EXT_MQTT_PORT", "1883"))
#EXT_TOPIC = os.getenv("EXT_TOPIC", "SourceTemp")

# Broker INTERNO (Destino Local)
INT_BROKER = os.getenv("INT_MQTT_ADDRESS", "mosquitto")
INT_PORT = int(os.getenv("INT_MQTT_PORT", "1883"))
#INT_TOPIC = os.getenv("INT_TOPIC", "Temp")

# --- PROMETHEUS ---
prom_msg_counter = Counter('number_msgs', 'Mensajes reenviados')
prom_temp_gauge = Gauge('temp', 'Temperatura [C]')

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOG = logging.getLogger("[mqtt-bridge]")

# Cliente interno (Global para acceder desde callbacks)
client_int = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

def on_connect_ext(client, userdata, flags, rc, properties=None):
    if rc == 0:
        LOG.info(f"Conectado al Broker EXTERNO ({EXT_BROKER})")
        client.subscribe("Temp")
        LOG.info(f"Suscrito a topic externo: {EXT_TOPIC}")
        LOG.info(client.is_connected())
    else:
        LOG.error(f"Fallo conexión externa: {rc}")

def on_message_ext(client, userdata, msg):
    """
    Cuando llega un mensaje de fuera:
    1. Se actualiza Prometheus.
    2. Se reenvía al Mosquitto local.
    """
    try:
        payload_str = msg.payload.decode('utf-8')
        # LOG.info(f"Recibido de fuera: {payload_str}")

        # 1. Actualizar Métricas (Simulando la lectura del sensor)
        try:
            val = float(payload_str)
            prom_msg_counter.inc()
            prom_temp_gauge.set(val)
        except ValueError:
            LOG.warning(f"Dato no numérico recibido: {payload_str}")

        # 2. Reenviar al Broker Local (Puente)
        if client_int.is_connected():
            client_int.publish("Temp", msg.payload, qos=0, retain=False)
            LOG.info(f"Reenviado -> Local Mosquitto [Temp]: {payload_str}")
        else:
            LOG.error("No se pudo reenviar: Broker interno desconectado")

    except Exception as e:
        LOG.error(f"Error procesando mensaje: {e}")

def main():
    # Iniciar servidor de métricas
    start_http_server(9000)
    
    # --- 1. CONEXIÓN INTERNA (Destino) ---
    try:
        LOG.info(f"Conectando al Broker INTERNO ({INT_BROKER})...")
        client_int.connect(INT_BROKER, INT_PORT, 60)
        client_int.loop_start() # Bucle en background para el interno
    except Exception as e:
        LOG.error(f"Fallo crítico conectando a Mosquitto local: {e}")
        sys.exit(1)

    # --- 2. CONEXIÓN EXTERNA (Origen) ---
    client_ext = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client_ext.on_connect = on_connect_ext
    client_ext.on_message = on_message_ext

    LOG.info(f"Conectando al Broker EXTERNO ({EXT_BROKER})...")
    
    while True:
        try:
            client_ext.connect(EXT_BROKER, EXT_PORT, 60)
            break
        except Exception as e:
            LOG.warning(f"Reintentando conexión externa en 5s... ({e})")
            time.sleep(5)

    # Bucle principal (mantiene vivo al script y escucha al externo)
    client_ext.loop_forever()

if __name__ == "__main__":
    main()