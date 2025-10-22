import os, io, json, uuid, logging, time
import re
from datetime import datetime
import asyncio
from telegram.error import NetworkError
import requests
import sys
import nest_asyncio
import pandas as pd
from pytz import timezone
import webbrowser
from flask import Flask, request
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, ConversationHandler, filters
)
from telegram.error import BadRequest
import gspread
from google.oauth2.service_account import Credentials
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import logging

nest_asyncio.apply()  # ✅ evita conflictos en Windows o VSCode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ======== ENV ========

load_dotenv()

#===========GOOGLE=============
# Google Sheets (opcional espejo)
SPREADSHEET_ID= "1imkrFoVgdzigEewp7St0wSUvnNdqz9BP69dxpCU1ucs"     # ID del spreadsheet

GOOGLE_IMAGES_FOLDER_ID = "1kI6dCip0ytIOH8jf1QazT3RFjtUvbB87"

GCP_SA_PATH = os.getenv("GCP_SA_PATH")     # ruta al service account JSON
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",  # subir/gestionar archivos creados por la app
    "https://www.googleapis.com/auth/drive"        # (opcional) si quieres buscar/leer en toda la unidad
]
# 🌍 API Key de Google Maps
GOOGLE_MAPS_API_KEY = "AIzaSyCLcEElUO_4khY4DmNeOLpqutk-yVFHF7c"

#===========ONE DRIVE=============

GRAPH = "https://graph.microsoft.com/v1.0"
TOKEN_FILE = "token_onedrive.json"

BOT_TOKEN = os.getenv("BOT_TOKEN")
DRIVE_ID = os.getenv("DRIVE_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
TENANT_ID = os.getenv("TENANT_ID")
REDIRECT_URI = os.getenv("REDIRECT_URI")

# 📁 Nombres específicos de tu entorno real
CARPETA_BASE = "REPORTE_INCIDENCIAS"
ARCHIVO_EXCEL = "Incidencias_PEXT.xlsx"

ORDENES_PATH = "REPORTE_INCIDENCIAS/Ordenes de trabajo.xlsx"


SHEET_NAME = "Hoja1"
ENCABEZADOS = [
    "USER_ID", "FECHA", "HORA", "PARTNER", "CUADRILLA", "TICKET", "DNI", "NOMBRE_CLIENTE",
    "NODO", "CODIGO_CAJA", "FOTO_CAJA", "FOTO_CAJA_ABIERTA", "FOTO_MEDICION", "LAT_CAJA", "LNG_CAJA",
    "DEPARTAMENTO", "PROVINCIA", "DISTRITO", "OBS"
]


OBS_OPCIONES = {
    "CTO": [
        "CTO sin potencia",
        "CTO con potencia degradada",
        "CTO Hurtada",
        "CTO sin facilidades",
        "CTO con puertos degradados",
        "CTO con puertos sin potencia",
        "CTO sin tapa",
        "Prevencion de CTO",
        "CTO - Habilitacion de puertos",
        "CTO con intermitencia",
        "CTO con conector mecanico",
        "Reposición de CTO",
    ],
    "NAP": [
        "NAP sin potencia",
        "NAP con potencia degradada",
        "NAP con puertos degradados",
        "NAP con puertos sin potencia",
        "NAP con rotulo equivocado",
        "NAP sin facilidades",
        "Prevencion de NAP",
        "NAP con intermitencia",
    ],
    "FAT": [
        "FAT sin potencia",
        "FAT con potencia degradada",
        "FAT sin facilidades",
        "FAT con puertos degradados",
        "FAT con puertos sin potencia",
        "FAT con intermitencia",
    ],
}

def _detectar_tipo_por_codigo(codigo: str) -> str | None:
    c = (codigo or "").upper()
    if "CTO" in c: return "CTO"
    if "NAP" in c: return "NAP"
    if "FAT" in c: return "FAT"
    return None


#=========== USUARIOS / GRUPO BOT =========
USUARIOS_DEV = {7175478712, 798153777}
GRUPO_SUPERVISION_ID = [-4621258308]  # si quieres enviar resumen al grupo, pon IDs aquí

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ======================================
# 🌐 FLASK APP PARA LOGIN INTERACTIVO
# ======================================
app = Flask(__name__)

@app.route("/")
def login():
    auth_url = (
        f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/authorize"
        f"?client_id={CLIENT_ID}"
        f"&response_type=code"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_mode=query"
        f"&scope=offline_access Files.ReadWrite User.Read"
    )
    webbrowser.open(auth_url)
    return "🌍 Abriendo login de Microsoft..."

@app.route("/callback")
def callback():
    code = request.args.get("code")
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    resp = requests.post(token_url, data=data)
    token_data = resp.json()

    with open(TOKEN_FILE, "w") as f:
        json.dump(token_data, f, indent=2)

    return f"✅ Autenticación completada. Tokens guardados en {TOKEN_FILE}."

# ======================================
# 🪪 TOKEN MANAGEMENT (Microsoft Graph)
# ======================================

def _save_tokens(tokens: dict):
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2, ensure_ascii=False)

def _load_tokens() -> dict:
    if not os.path.exists(TOKEN_FILE):
        raise RuntimeError("❌ Falta token_onedrive.json. Ejecuta py main.py --auth para generar el refresh_token.")
    with open(TOKEN_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def get_access_token() -> str:
    """Obtiene un nuevo access_token a partir del refresh_token."""
    toks = _load_tokens()
    refresh_token = toks.get("refresh_token")
    if not refresh_token:
        raise RuntimeError("❌ token_onedrive.json no tiene refresh_token. Reautentica con --auth.")

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
        "redirect_uri": REDIRECT_URI,
        "scope": "offline_access Files.ReadWrite.All Sites.ReadWrite.All User.Read"
    }
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    r = requests.post(url, data=data, timeout=30)
    j = r.json()
    if r.status_code != 200:
        logger.error("❌ Error refrescando token: %s", r.text)
        raise RuntimeError("No se pudo refrescar el token")

    if "refresh_token" in j:
        _save_tokens(j)
    return j["access_token"]

def _headers(token, json_ct=True):
    h = {"Authorization": f"Bearer {token}"}
    if json_ct:
        h["Content-Type"] = "application/json"
    return h

def _item_url(path_suffix):
    return f"{GRAPH}/me/drive/root:/{CARPETA_BASE}/{ARCHIVO_EXCEL}:{path_suffix}"

# ======================================
# ☁️ GOOGLE SHEETS SYNC
# ======================================

def _gs_connect():
    """Conecta a Google Sheets usando Service Account"""
    try:
        creds = Credentials.from_service_account_file(
            "credentials.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        gc = gspread.authorize(creds)
        sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
        return sheet
    except gspread.SpreadsheetNotFound:
        logger.error("❌ No se encontró el Google Sheet. Verifica el SPREADSHEET_ID.")
        raise
    except Exception as e:
        logger.error(f"❌ Error conectando con Google Sheets: {e}")
        raise

def gs_ensure_headers(sheet):
    """Verifica y crea los encabezados si no existen, sin borrar datos previos."""
    try:
        expected_headers = ENCABEZADOS
        current = sheet.row_values(1)

        # Si la hoja está vacía (sin encabezados)
        if not current:
            logger.info("📄 Hoja vacía. Creando encabezados...")
            sheet.update([expected_headers], "A1:S1")
            logger.info("✅ Encabezados creados correctamente.")
            return

        # Si los encabezados difieren parcialmente (ajustar columnas sin borrar contenido)
        if current != expected_headers:
            logger.info("🧾 Corrigiendo encabezados sin borrar contenido...")
            # Solo actualiza celdas de encabezado, no borra filas previas
            for i, val in enumerate(expected_headers, start=1):
                if i > len(current) or current[i - 1] != val:
                    sheet.update_cell(1, i, val)
            logger.info("✅ Encabezados actualizados sin borrar filas previas.")
        else:
            logger.debug("🟢 Encabezados ya están correctos.")

    except Exception as e:
        logger.error(f"❌ Error asegurando encabezados en Google Sheets: {e}")


def gs_append_row(fila):
    """Agrega una fila al Google Sheet con tolerancia a errores"""
    try:
        if len(fila) < len(ENCABEZADOS): fila += [""] * (len(ENCABEZADOS) - len(fila))
        elif len(fila) > len(ENCABEZADOS): fila = fila[:len(ENCABEZADOS)]

        sheet = _gs_connect()
        try:
            gs_ensure_headers(sheet)
        except Exception as e:
            logger.warning(f"⚠️ No se pudieron asegurar encabezados: {e}")

        sheet.append_row(fila, value_input_option="USER_ENTERED")
        logger.info("☁️ Fila reflejada correctamente en Google Sheets.")

    except gspread.SpreadsheetNotFound:
        logger.error("❌ ID de Google Sheet inválido o inexistente.")
    except gspread.exceptions.APIError as e:
        if "PERMISSION_DENIED" in str(e):
            logger.error("🚫 Service Account sin acceso. Compártelo con permisos de editor.")
        else:
            logger.error(f"❌ Error API Google Sheets: {e}")
    except Exception as e:
        logger.error(f"⚠️ Error reflejando en Google Sheets: {e}")

# ======================================
# 💾 ESCRITURA DIRECTA EN ONEDRIVE
# ======================================

def append_row_online(fila: list) -> bool:
    """📘 Añade una fila en el Excel de OneDrive usando Graph API (sin WAC)."""
    try:
        token = get_access_token()
        fila = list(fila)
        if len(fila) < len(ENCABEZADOS):
            fila += [""] * (len(ENCABEZADOS) - len(fila))

        # 1️⃣ Asegurar encabezados
        last_col = get_column_letter(len(ENCABEZADOS))
        rango = f"A1:{last_col}1"
        url = _item_url(f"/workbook/worksheets('{SHEET_NAME}')/range(address='{rango}')")
        body = {"values": [ENCABEZADOS]}
        requests.patch(url, headers=_headers(token), json=body)

        # 2️⃣ Obtener la siguiente fila libre
        used = requests.get(_item_url(f"/workbook/worksheets('{SHEET_NAME}')/usedRange"), headers=_headers(token))
        next_row = len(used.json().get("values", [])) + 1 if used.status_code == 200 else 2

        # 3️⃣ Escribir la nueva fila
        rango = f"A{next_row}:{last_col}{next_row}"
        url = _item_url(f"/workbook/worksheets('{SHEET_NAME}')/range(address='{rango}')")
        body = {"values": [fila]}
        r = requests.patch(url, headers=_headers(token), json=body)

        if r.status_code in (200, 201):
            logger.info(f"✅ Fila {next_row} añadida correctamente en OneDrive.")
            return True
        else:
            logger.error(f"❌ Error añadiendo fila: {r.status_code} {r.text}")
            return False

    except Exception as e:
        logger.error(f"❌ Error general en append_row_online: {e}")
        return False

# ======================================
# 🧱 CREACIÓN DE CARPETA Y ARCHIVO EN ONEDRIVE
# ======================================

def ensure_excel_exists():
    """Verifica si el Excel existe en OneDrive; si no, lo crea con encabezados."""
    token = get_access_token()
    base_headers = _headers(token)
    graph = "https://graph.microsoft.com/v1.0"

    folder_name = CARPETA_BASE
    file_name = ARCHIVO_EXCEL

    # 1️⃣ Verificar carpeta
    folder_url = f"{graph}/me/drive/root:/{folder_name}"
    r = requests.get(folder_url, headers=base_headers)
    if r.status_code == 404:
        logger.info(f"📁 Carpeta {folder_name} no existe, creando...")
        create_folder_url = f"{graph}/me/drive/root/children"
        data = {"name": folder_name, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"}
        resp = requests.post(create_folder_url, headers=base_headers, json=data)
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"❌ Error creando carpeta: {resp.text}")
        logger.info(f"✅ Carpeta {folder_name} creada.")

    # 2️⃣ Verificar archivo Excel
    file_url = f"{graph}/me/drive/root:/{folder_name}/{file_name}"
    r = requests.get(file_url, headers=base_headers)
    if r.status_code == 200:
        logger.info(f"📄 El archivo {file_name} ya existe en OneDrive.")
        return True

    # 3️⃣ Crear archivo nuevo vacío
    logger.info(f"📘 Creando archivo {file_name} en OneDrive...")
    create_url = f"{graph}/me/drive/root:/{folder_name}/{file_name}:/content"
    wb = Workbook()
    ws = wb.active
    ws.append(ENCABEZADOS)
    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)

    resp = requests.put(create_url, headers=_headers(token, json_ct=False), data=bio.read())
    if resp.status_code in (200, 201):
        logger.info(f"✅ Archivo {file_name} creado correctamente en OneDrive.")
    else:
        raise RuntimeError(f"❌ Error creando Excel: {resp.text}")

    return True

# ============================================
# 🔄 SINCRONIZACIÓN COMPLETA ONEDRIVE → GOOGLE SHEETS
# ============================================

def sync_full_excel_to_google_sheets(token):
    """
    📤 Sincroniza TODO el contenido del Excel en OneDrive al Google Sheet reflejo.
    Incluye encabezados y todas las filas de datos.
    """
    try:
        # 1️⃣ Descargar Excel desde OneDrive
        url = f"{GRAPH}/drives/{DRIVE_ID}/root:/REPORTE_INCIDENCIAS/Incidencias_PEXT.xlsx:/content"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers, timeout=60)

        if resp.status_code != 200:
            raise RuntimeError(f"❌ Error descargando Excel: {resp.status_code} {resp.text}")

        df = pd.read_excel(io.BytesIO(resp.content))
        df = df.fillna("").astype(str)  # evita errores al convertir

        # 2️⃣ Conectar con Google Sheets
        creds = Credentials.from_service_account_file(
            "credentials.json",
            scopes=["https://www.googleapis.com/auth/spreadsheets"]
        )
        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        # 3️⃣ Limpiar hoja existente
        sheet.values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME_GOOGLE}!A:Z"
        ).execute()

        # 4️⃣ Subir encabezados + filas
        values = [df.columns.tolist()] + df.values.tolist()
        body = {"values": values}

        sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME_GOOGLE}!A1",
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()

        logger.info(f"✅ Google Sheet sincronizado con {len(df)} filas desde OneDrive.")
    except Exception as e:
        logger.error(f"⚠️ Error en sincronización total Google Sheets: {e}")


# ============================================
# 📸 SUBIDA DE FOTOS A GOOGLE DRIVE (VERSIÓN BLINDADA)
# ============================================

def ensure_google_folder_imagenes():
    """
    Verifica si existe la carpeta 'IMAGENES' (por ID o nombre).
    Si no existe, la crea y devuelve su ID.
    Compatible con unidades compartidas (supportsAllDrives=True).
    """
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
        service = build("drive", "v3", credentials=creds)

        # 1️⃣ Verificar si el ID definido existe y es accesible
        if GOOGLE_IMAGES_FOLDER_ID:
            try:
                f = service.files().get(
                    fileId=GOOGLE_IMAGES_FOLDER_ID,
                    fields="id, name",
                    supportsAllDrives=True
                ).execute()
                logger.info(f"📁 Carpeta IMAGENES existente: {f['id']} ({f['name']})")
                return GOOGLE_IMAGES_FOLDER_ID
            except Exception:
                logger.warning("⚠️ La carpeta IMAGENES con el ID definido no existe o no es accesible. Se buscará o creará una nueva.")

        # 2️⃣ Buscar por nombre 'IMAGENES' en el Drive
        query = "name='IMAGENES' and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        folders = results.get("files", [])

        if folders:
            folder_id = folders[0]["id"]
            logger.info(f"📁 Carpeta IMAGENES encontrada por nombre: {folder_id}")
            return folder_id

        # 3️⃣ Crear la carpeta si no existe
        metadata = {
            "name": "IMAGENES",
            "mimeType": "application/vnd.google-apps.folder"
        }
        folder = service.files().create(
            body=metadata,
            fields="id",
            supportsAllDrives=True
        ).execute()
        folder_id = folder["id"]
        logger.info(f"🆕 Carpeta IMAGENES creada en Google Drive: {folder_id}")
        return folder_id

    except Exception as e:
        logger.error(f"❌ Error asegurando carpeta IMAGENES: {e}")
        return None


def upload_image_to_google_drive(file_bytes: bytes, filename: str):
    """
    Sube imagen a la carpeta IMAGENES en Google Drive (creándola si no existe)
    y devuelve su enlace público.
    Compatible con unidades compartidas (supportsAllDrives=True).
    """
    try:
        creds = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
        service = build("drive", "v3", credentials=creds)

        # 🗂 Obtener o crear carpeta IMAGENES
        folder_id = ensure_google_folder_imagenes()
        if not folder_id:
            logger.error("❌ No se pudo obtener ni crear la carpeta IMAGENES.")
            return None

        # 📤 Subir la imagen
        file_metadata = {
            "name": filename,
            "parents": [folder_id],
            "mimeType": "image/jpeg"
        }
        media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype="image/jpeg", resumable=True)
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True
        ).execute()

        # 🔓 Hacer pública la imagen
        service.permissions().create(
            fileId=file["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True
        ).execute()

        web_link = file["webViewLink"]
        logger.info(f"✅ Imagen subida correctamente a Google Drive: {web_link}")
        return web_link

    except Exception as e:
        logger.error(f"❌ Error subiendo imagen a Google Drive: {e}")
        return None



# ======================================
# 🗂️ VERIFICAR CARPETA IMAGENES ANTES DE INICIAR EL BOT
# ======================================

def verificar_carpeta_imagenes_inicial():
    try:
        logger.info("🔎 Verificando carpeta IMAGENES antes de iniciar el bot...")
        folder_id = ensure_google_folder_imagenes()
        if folder_id:
            logger.info(f"✅ Carpeta IMAGENES lista para usar: {folder_id}")
        else:
            logger.error("❌ No se pudo verificar o crear la carpeta IMAGENES. Revisa tus credenciales o permisos.")
    except Exception as e:
        logger.error(f"💥 Error al verificar carpeta IMAGENES: {e}")


ORDENES_DF = None
ORDENES_LAST_TS = 0


# ============================================
# 📦 CARGA DE CAJAS-NODOS
# ============================================

CAJAS_NODOS = {}

def cargar_cajas_nodos():
    """Descarga CAJAS_NODOS.xlsx y llena el dict {CODIGO_CAJA: NODO} desde OneDrive/SharePoint."""
    try:
        logger.info("🚀 Cargando CAJAS_NODOS desde SharePoint...")
        token = get_access_token()

        url = f"{GRAPH}/drives/{DRIVE_ID}/root:/REPORTE_INCIDENCIAS/CAJAS_NODOS.xlsx:/content"
        r = requests.get(url, headers=_headers(token, json_ct=False), timeout=40)

        if r.status_code != 200:
            raise RuntimeError(f"No se pudo descargar CAJAS_NODOS.xlsx: {r.status_code} {r.text}")

        with io.BytesIO(r.content) as fh:
            df = pd.read_excel(fh)

        df["CODIGO_CAJA"] = df["CODIGO_CAJA"].astype(str).str.strip().str.upper()
        df["NODO"] = df["NODO"].astype(str).str.strip()

        global CAJAS_NODOS
        CAJAS_NODOS = dict(zip(df["CODIGO_CAJA"], df["NODO"]))
        logger.info("✅ Cargados %d mapeos de CAJAS_NODOS.", len(CAJAS_NODOS))

    except Exception as e:
        logger.error(f"❌ Error cargando CAJAS_NODOS.xlsx: {e}")

def obtener_nodo_por_codigo(codigo: str) -> str:
    try:
        return CAJAS_NODOS.get(codigo.strip().upper(), "")
    except Exception:
        return ""

# ======================================
# 🪪 FUNCIONES AUXILIARES MICROSOFT GRAPH
# ======================================
def _graph_headers(token, json_ct=True):
    """
    Genera los encabezados HTTP para usar con Microsoft Graph API.
    """
    headers = {"Authorization": f"Bearer {token}"}
    if json_ct:
        headers["Content-Type"] = "application/json"
    return headers


def cargar_ordenes_trabajo(force: bool = False):
    """
    Carga/recarga ORDENES_DF desde OneDrive/SharePoint sin depender de nombres de columna.
    Detecta automáticamente la hoja correcta y limpia todas las columnas.
    """
    global ORDENES_DF, ORDENES_LAST_TS
    try:
        now = time.time()
        if not force and ORDENES_DF is not None and (now - ORDENES_LAST_TS) < (20 * 60):
            return  # Cache menor a 20 minutos

        logger.info("🚀 Cargando Ordenes de trabajo desde OneDrive/SharePoint...")

        token = get_access_token()
        url = f"{GRAPH}/drives/{DRIVE_ID}/root:/{ORDENES_PATH}:/content"
        r = requests.get(url, headers=_graph_headers(token), timeout=40)
        if r.status_code != 200:
            raise RuntimeError(f"No se pudo descargar Ordenes de trabajo.xlsx: {r.status_code} {r.text}")

        with io.BytesIO(r.content) as fh:
            try:
                # 📄 Leer todas las hojas y elegir la más grande (la que tenga más datos)
                all_sheets = pd.read_excel(fh, sheet_name=None)
                df = max(all_sheets.values(), key=lambda x: x.shape[0] * x.shape[1])
                logger.info(f"📄 Hoja seleccionada automáticamente: {df.shape[0]} filas, {df.shape[1]} columnas")
            except Exception:
                # ⚠️ Si falla, leer sin encabezados (modo crudo)
                fh.seek(0)
                df = pd.read_excel(fh, header=None)
                df.columns = [f"COLUMN_{i}" for i in range(df.shape[1])]
                logger.warning("⚠️ Archivo leído sin encabezados; columnas asignadas genéricamente.")

        # 🔹 Limpieza avanzada de encabezados
        df.columns = [
            str(c)
            .upper()
            .replace("ORDENES[", "")
            .replace("]", "")
            .replace("[", "")
            .replace(" ", "_")
            .strip()
            for c in df.columns
        ]

        # 🔹 Limpieza general de datos
        df = df.fillna("").map(lambda x: str(x).strip())

        ORDENES_DF = df
        ORDENES_LAST_TS = now
        logger.info("✅ Ordenes de trabajo cargado correctamente (%d filas, %d columnas).", len(df), len(df.columns))

    except Exception as e:
        logger.error("❌ Error cargando Ordenes de trabajo.xlsx: %s", e)



def buscar_datos_ticket(ticket: str) -> dict | None:
    """
    Busca un ticket en la columna D del DataFrame ORDENES_DF.
    Si lo encuentra, devuelve los datos de la misma fila:
    A → NOMBRE_CLIENTE, B → CUADRILLA, C → DNI, E → PARTNER.
    """
    try:
        if ORDENES_DF is None:
            cargar_ordenes_trabajo(force=True)
        if ORDENES_DF is None or ORDENES_DF.empty:
            logger.warning("⚠️ ORDENES_DF vacío, no se puede buscar ticket.")
            return None

        t = str(ticket).strip().upper()
        df = ORDENES_DF.copy()

        # Normalizar nombres de columnas a A, B, C, D, E por posición
        df.columns = [f"COL_{i}" for i in range(len(df.columns))]

        # Buscar el ticket en la columna D (COL_3)
        df["COL_3"] = df["COL_3"].astype(str).str.upper().str.strip()
        fila = df[df["COL_3"].str.contains(t, na=False)]

        if fila.empty:
            logger.info(f"🔍 Ticket '{t}' no encontrado en la columna D.")
            return None

        row = fila.iloc[0]

        datos = {
            "NOMBRE_CLIENTE": str(row.get("COL_0", "")).strip(),
            "CUADRILLA": str(row.get("COL_1", "")).strip(),
            "DNI": str(row.get("COL_2", "")).strip(),
            "TICKET": str(row.get("COL_3", "")).strip(),
            "PARTNER": str(row.get("COL_4", "")).strip(),
        }

        logger.info(f"✅ Ticket '{t}' encontrado. Datos: {datos}")
        return datos

    except Exception as e:
        logger.error(f"❌ Error buscando ticket '{ticket}': {e}")
        return None


#=========================================================


async def _job_recarga_ordenes(context: ContextTypes.DEFAULT_TYPE):
    cargar_ordenes_trabajo(force=True)


# ================== PASOS ===============================================================================
PASOS = {
    "TICKET": {
        "tipo": "texto",
        "mensaje": "🎫 Ingrese el número de *TICKET* a registrar:",
        "siguiente": "CODIGO_CAJA",
    },
    "CODIGO_CAJA": {
        "tipo": "texto",
        "mensaje": "🏷 Ingresa el *Código de la CTO/NAP/FAT*:",
        "siguiente": "UBICACION_CTO",
    },
    "UBICACION_CTO": {
        "tipo": "ubicacion",
        "mensaje": "📍 Envíe la ubicación de la CTO/NAP/FAT:",
        "lat_key": "LAT_CAJA",
        "lng_key": "LNG_CAJA",
        "siguiente": "FOTO_CAJA",
    },
    "FOTO_CAJA": {
        "tipo": "foto",
        "mensaje": "📸 Envía *foto de la CTO/NAP/FAT con rotulo visible*:",
        "siguiente": "FOTO_CAJA_ABIERTA",
    },
    "FOTO_CAJA_ABIERTA": {
        "tipo": "foto",
        "mensaje": "📸 Envía *foto de la CTO/NAP/FAT abierta* mostrando puertos visibles:",
        "siguiente": "FOTO_MEDICION",
    },
    "FOTO_MEDICION": {
        "tipo": "foto",
        "mensaje": "📸 Envía *foto de la potencia óptica en dBm. & λ 1490 nm.* del puerto asignado:",
        "siguiente": "OBS",
    },
    "OBS": {
        "tipo": "menu",
        "mensaje": "🧭 Selecciona el tipo de observación en CTO / NAP / FAT:",
        "instruccion": "📋 Usa el menú para elegir el tipo de observación.",
        "siguiente": "RESUMEN_FINAL",
    }
}


PASOS_LISTA = list(PASOS.keys())

ETIQUETAS = {
    "TICKET": "🎫 Ticket",
    "DNI": "🪪 DNI",
    "NOMBRE_CLIENTE": "👤 Cliente",
    "CODIGO_CAJA": "🏷 Código CTO/NAP/FAT",
    "UBICACION_CTO": "📍 Ubicación CTO/NAP/FAT",
    "FOTO_CAJA": "📸 Foto CTO/NAP/FAT (Exterior)",
    "FOTO_CAJA_ABIERTA": "📦 Foto de CTO/NAP/FAT (Interior)",
    "FOTO_MEDICION": "📏 Foto de medición óptica (dBm)",
    "OBS": "📝 Observaciones"
}




# ================== UTILS ==================
def get_fecha_hora():
    lima = timezone("America/Lima")
    now = datetime.now(lima)
    return now.strftime("%Y-%m-%d"), now.strftime("%H:%M:%S")

def geocodificar(lat, lng):
    """Devuelve Departamento, Provincia y Distrito usando Google Maps API"""
    if not GOOGLE_MAPS_API_KEY:
        logger.error("❌ GOOGLE_MAPS_API_KEY no está definido.")
        return "-", "-", "-"

    url = (
        f"https://maps.googleapis.com/maps/api/geocode/json?"
        f"latlng={lat},{lng}&key={GOOGLE_MAPS_API_KEY}&language=es"
    )

    try:
        resp = requests.get(url, timeout=12).json()
    except Exception as e:
        logger.error(f"❌ Error en request a Google Maps: {e}")
        return "-", "-", "-"

    if resp.get("status") != "OK" or not resp.get("results"):
        logger.error(f"❌ Geocoding falló → {resp.get('status')}, {resp.get('error_message')}")
        return "-", "-", "-"

    comps = resp["results"][0]["address_components"]

    depto, prov, distrito = "-", "-", "-"
    for c in comps:
        t = c.get("types", [])
        if "administrative_area_level_1" in t:   # Departamento (ej. Lima)
            depto = c.get("long_name", "-")
        elif "administrative_area_level_2" in t: # Provincia
            prov = c.get("long_name", "-")
        elif "locality" in t or "administrative_area_level_3" in t or "sublocality_level_1" in t:
            distrito = c.get("long_name", "-")

    # ✅ Validación de respaldo
    if distrito == "-" and len(resp["results"]) > 1:
        # Algunos resultados secundarios tienen mayor detalle
        for alt in resp["results"][1:]:
            for c in alt.get("address_components", []):
                if "locality" in c.get("types", []):
                    distrito = c.get("long_name", "-")

    logger.info(f"📍 Geocodificado correctamente: {depto}, {prov}, {distrito}")
    return depto, prov, distrito

def obtener_ubicacion(lat, lng):
    """Devuelve departamento, provincia y distrito usando Google Maps API"""
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&key={GOOGLE_MAPS_API_KEY}&language=es"
    response = requests.get(url)
    data = response.json()

    departamento, provincia, distrito = "-", "-", "-"
    if data.get("status") == "OK" and data.get("results"):
        for comp in data["results"][0]["address_components"]:
            if "administrative_area_level_1" in comp["types"]:
                departamento = comp["long_name"]
            elif "administrative_area_level_2" in comp["types"]:
                provincia = comp["long_name"]
            elif "locality" in comp["types"] or "administrative_area_level_3" in comp["types"]:
                distrito = comp["long_name"]
    return departamento, provincia, distrito


# ================== START ==================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in GRUPO_SUPERVISION_ID:
        return ConversationHandler.END

    registro = context.user_data.get("registro", {})
    if registro.get("ACTIVO", False):
        paso_actual = registro.get("PASO_ACTUAL", PASOS_LISTA[0])
        await update.message.reply_text(
            f"⚠️ Ya tienes un registro en curso.\n\n"
            f"📌 Estás en el paso: *{ETIQUETAS.get(paso_actual, paso_actual)}*.\n\n"
            f"👉 Responde lo solicitado o usa /cancel para anular.",
            parse_mode="Markdown"
        )
        return paso_actual

    # Mensaje de bienvenida
    instrucciones = (
        "👋 *Bienvenido al Bot de Incidencias*\n\n"
        "• Usa /registro para iniciar un nuevo registro.\n"
        "• Usa /cancel para cancelar un registro en curso.\n\n"
        "‼️ Si ya tienes un registro activo, no podrás iniciar otro."
    )
    await update.message.reply_text(instrucciones, parse_mode="Markdown")
    return ConversationHandler.END

async def comando_registro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if chat_id in GRUPO_SUPERVISION_ID:
        return ConversationHandler.END

    reg = context.user_data.get("registro")
    if reg and reg.get("ACTIVO", False):
        paso_actual = reg.get("PASO_ACTUAL", PASOS_LISTA[0])
        await update.message.reply_text(
            f"⚠️ Ya tienes un registro en curso.\n\n"
            f"📌 Estás en el paso: *{ETIQUETAS.get(paso_actual, paso_actual)}*.\n\n"
            f"👉 Responde lo solicitado o usa /cancel para anular.",
            parse_mode="Markdown"
        )
        return paso_actual

    # Crear registro nuevo
    context.user_data["registro"] = {
        "USER_ID": user_id,
        "ID_REGISTRO": str(uuid.uuid4())[:8],
        "ACTIVO": True,
        "PASO_ACTUAL": "TICKET",
    }
    await update.message.reply_text("🎫 Ingrese el *TICKET* a registrar:", parse_mode="Markdown")
    return "TICKET"


# ================== MANEJAR PASO (CORREGIDO) ==================
async def manejar_paso(update: Update, context: ContextTypes.DEFAULT_TYPE, paso: str):
    chat_id = update.effective_chat.id

    # 🚫 Evita respuestas del grupo de supervisión
    if chat_id in GRUPO_SUPERVISION_ID:
        return ConversationHandler.END

    registro = context.user_data.setdefault("registro", {})
    paso_cfg = PASOS.get(paso, {"tipo": "texto"})  # asume tu dict PASOS tiene 'tipo' y 'siguiente'

    # 🔸 CORRECCIÓN CLAVE 🔸
    # Si el paso actual es OBS → abrir menú de observaciones en lugar de pedir texto
    if paso == "OBS":
        logger.info("🟣 Entrando a menú de observaciones desde manejar_paso()")
        await mostrar_menu_obs(chat_id, context, tipo=None)
        registro["PASO_ACTUAL"] = "OBS_TIPO"
        return "OBS_TIPO"

    # ─────────────────────────────────────────────────────────────
    # Helper: marcar si venimos de resumen y limpiar el flag
    # ─────────────────────────────────────────────────────────────
    def _marcar_origen_resumen(reg):
        if reg.get("DESDE_RESUMEN", False):
            reg["VOLVER_A_RESUMEN"] = True       # ← marca intención de regresar al resumen tras confirmar
            reg["DESDE_RESUMEN"] = False         # ← reset inmediato para NO disparar resúmenes fuera de lugar

    # ─────────────────────────────────────────────────────────────
    # 1) TICKET (texto)
    # ─────────────────────────────────────────────────────────────
    if paso == "TICKET":
        if not update.message or not update.message.text:
            await update.message.reply_text("⚠️ Debes enviar un número de ticket válido.")
            return paso

        ticket = update.message.text.strip().upper()
        registro["TICKET"] = ticket

        _marcar_origen_resumen(registro)

        # Buscar datos del ticket
        datos = buscar_datos_ticket(ticket)
        if datos:
            registro["NOMBRE_CLIENTE"] = datos.get("NOMBRE_CLIENTE", "-")
            registro["DNI"]            = datos.get("DNI", "-")
            registro["CUADRILLA"]      = datos.get("CUADRILLA", "-")
            registro["PARTNER"]        = datos.get("PARTNER", "-")
        else:
            await update.message.reply_text(
                "⚠️ Ticket no encontrado en las órdenes actuales.\nPor favor vuelve a ingresar un *Ticket válido*:",
                parse_mode="Markdown"
            )
            return "TICKET"

        # Mostrar confirmación con botoneras
        msg = (
            f"✅ Datos encontrados para Ticket {ticket}:\n\n"
            f"👤 Cliente: {registro.get('NOMBRE_CLIENTE', '-')}\n"
            f"🪪 DNI: {registro.get('DNI', '-')}\n"
            f"👷 Cuadrilla: {registro.get('CUADRILLA', '-')}\n"
            f"🏢 Partner: {registro.get('PARTNER', '-')}\n\n"
            f"¿Es correcto el *Ticket* ingresado?"
        )
        keyboard = [[
            InlineKeyboardButton("✅ Confirmar", callback_data="CONFIRMAR_TICKET"),
            InlineKeyboardButton("✏️ Corregir",  callback_data="CORREGIR_TICKET"),
        ]]
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        registro["PASO_ACTUAL"] = "TICKET"
        return "CONFIRMAR"

    # ─────────────────────────────────────────────────────────────
    # 2) CODIGO_CAJA (texto → buscar NODO + detectar tipo)
    # ─────────────────────────────────────────────────────────────
    if paso == "CODIGO_CAJA":
        if not update.message or not update.message.text:
            await update.message.reply_text("⚠️ Debes enviar un código de CTO/NAP/FAT válido.")
            return paso

        _marcar_origen_resumen(registro)

        codigo = update.message.text.strip().upper()
        registro["CODIGO_CAJA"] = codigo

        try:
            nodo = obtener_nodo_por_codigo(codigo)  # ← tu función
        except Exception as e:
            nodo = None
            logger.error(f"❌ Error obteniendo nodo para {codigo}: {e}")

        registro["NODO"] = nodo or "-"

        if nodo:
            await update.message.reply_text(f"📡 Nodo encontrado: *{nodo}*", parse_mode="Markdown")

        # Detección automática de tipo de observación (opcional)
        try:
            tipo_detectado = _detectar_tipo_por_codigo(codigo)  # ← tu función
        except Exception as e:
            tipo_detectado = None
            logger.warning(f"⚠️ No se pudo detectar tipo por código: {e}")

        if tipo_detectado:
            registro["OBS_TIPO"] = tipo_detectado
            await update.message.reply_text(f"🧩 Tipo detectado automáticamente: *{tipo_detectado}*", parse_mode="Markdown")

        # Botonera
        msg = (
            f"🏷 *Código CTO/NAP/FAT:* {registro['CODIGO_CAJA']}\n"
            f"📡 *Nodo:* {registro.get('NODO','-')}\n\n"
            f"¿Deseas confirmar o corregir?"
        )
        keyboard = [[
            InlineKeyboardButton("✅ Confirmar", callback_data="CONFIRMAR_CODIGO_CAJA"),
            InlineKeyboardButton("✏️ Corregir",  callback_data="CORREGIR_CODIGO_CAJA"),
        ]]
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        registro["PASO_ACTUAL"] = "CODIGO_CAJA"
        return "CONFIRMAR"


    # ─────────────────────────────────────────────────────────────
    # 3) UBICACIONES (cubre pasos de tipo 'ubicacion' vía PASOS)
    #    Debes tener en PASOS[paso] las keys: lat_key, lng_key, siguiente
    # ─────────────────────────────────────────────────────────────
    if paso_cfg["tipo"] == "ubicacion":
        if not update.message or not update.message.location:
            await update.message.reply_text("⚠️ Debe enviar una *ubicación GPS* válida.")
            return paso

        # 💡 NUEVO: detectar si viene desde resumen final
        if registro.get("DESDE_RESUMEN", False):
            registro["VOLVER_A_RESUMEN"] = True
            registro["DESDE_RESUMEN"] = False
            logger.info("🔁 Corrección de ubicación desde resumen → volverá al resumen final tras confirmar")

        _marcar_origen_resumen(registro)

        lat = update.message.location.latitude
        lng = update.message.location.longitude
        registro[paso_cfg["lat_key"]] = lat
        registro[paso_cfg["lng_key"]] = lng

        # Geocodificación
        try:
            dep, prov, dist = geocodificar(lat, lng)  # ← tu función
        except Exception as e:
            logger.error(f"❌ Error geocodificando: {e}")
            dep = prov = dist = "-"

        registro["DEPARTAMENTO"] = dep or "-"
        registro["PROVINCIA"]    = prov or "-"
        registro["DISTRITO"]     = dist or "-"

        # 📍 Mensaje con mapa y botones de confirmación/corrección
        mensaje_ubicacion = (
            f"✅ 📍 *Ubicación CTO/NAP/FAT confirmada:* ({lat:.6f}, {lng:.6f})\n"
            f"🧭 *Lugar de Incidencia:* {registro['DEPARTAMENTO']}, "
            f"{registro['PROVINCIA']}, {registro['DISTRITO']}\n"
            f"🌍 [Ver ubicación CTO](https://maps.google.com/?q={lat},{lng})"
        )

        keyboard = [[
            InlineKeyboardButton("✅ Confirmar", callback_data=f"CONFIRMAR_{paso}"),
            InlineKeyboardButton("✏️ Corregir",  callback_data=f"CORREGIR_{paso}"),
        ]]

        await update.message.reply_text(
            mensaje_ubicacion,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
            disable_web_page_preview=True
        )

        registro["PASO_ACTUAL"] = paso
        return "CONFIRMAR"


    # ─────────────────────────────────────────────────────────────
    # 4) FOTOS (cubre pasos de tipo 'foto' vía PASOS)
    # ─────────────────────────────────────────────────────────────
    if paso_cfg["tipo"] == "foto":
        _marcar_origen_resumen(registro)

        if "ID_REGISTRO" not in registro:
            registro["ID_REGISTRO"] = datetime.now().strftime("%Y%m%d%H%M%S")

        filename = f"{paso}_{registro['ID_REGISTRO']}.jpg"
        file_bytes = None

        # Aceptar photo o document (imagen)
        if update.message.photo:
            photo = update.message.photo[-1]
            file = await photo.get_file()
            file_bytes = await file.download_as_bytearray()
        elif update.message.document and update.message.document.mime_type and update.message.document.mime_type.startswith("image/"):
            file = await update.message.document.get_file()
            file_bytes = await file.download_as_bytearray()
            filename = update.message.document.file_name or filename
        else:
            await update.message.reply_text("⚠️ Debe enviar una *foto* (imagen o archivo de imagen).")
            return paso

        # Subir (o procesar) la foto
        try:
            link_google = upload_image_to_google_drive(file_bytes, filename)  # ← tu función
            if not link_google:
                await update.message.reply_text("⚠️ No se pudo procesar la foto, por favor vuelve a enviarla.")
                return paso
            # Guarda solo lo necesario para ahorrar RAM
            registro[paso] = link_google
            # Si deseas conservar bytes:
            # registro[f"{paso}_BYTES"] = file_bytes
        except Exception as e:
            logger.error(f"❌ Error subiendo imagen: {e}")
            await update.message.reply_text("⚠️ Hubo un problema con la foto. Intenta nuevamente.")
            return paso

        # Botonera
        keyboard = [[
            InlineKeyboardButton("✅ Confirmar", callback_data=f"CONFIRMAR_{paso}"),
            InlineKeyboardButton("✏️ Corregir",  callback_data=f"CORREGIR_{paso}"),
        ]]
        await update.message.reply_text(
            "📸 Foto recibida. ¿Deseas *confirmarla* o *volver a tomarla*?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        registro["PASO_ACTUAL"] = paso
        return "CONFIRMAR"

    # ─────────────────────────────────────────────────────────────
    # 5) TEXTO genérico (ej. OBSERVACION u otros campos de texto)
    # ─────────────────────────────────────────────────────────────
    if paso_cfg["tipo"] == "texto":
        # ⚙️ Excepción clave: si el paso es OBS, no pedir texto sino abrir menú
        if paso == "OBS":
            logger.info("🟣 Derivando a menú de observaciones desde bloque de texto")
            await mostrar_menu_obs(chat_id, context, tipo=None)
            registro["PASO_ACTUAL"] = "OBS_TIPO"
            return "OBS_TIPO"

        # ⚠️ Validación normal de texto
        if not update.message or not update.message.text:
            await update.message.reply_text("⚠️ Solo se acepta *texto* en este paso.")
            return paso

        _marcar_origen_resumen(registro)

        valor = update.message.text.strip()
        registro[paso] = valor

        # 🔘 Confirmar / Corregir botones
        keyboard = [[
            InlineKeyboardButton("✅ Confirmar", callback_data=f"CONFIRMAR_{paso}"),
            InlineKeyboardButton("✏️ Corregir",  callback_data=f"CORREGIR_{paso}"),
        ]]
        await update.message.reply_text(
            f"📝 *{paso.replace('_',' ')}* registrado:\n{valor}\n\n¿Confirmas o corriges?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

        registro["PASO_ACTUAL"] = paso
        return "CONFIRMAR"

    # Por si acaso
    await update.message.reply_text("⚠️ Paso no reconocido. Intenta nuevamente.")
    return paso


# ============================================================
# ✅ CONFIRMAR_<PASO> → separa flujos (resumen vs normal)
# ============================================================
async def manejar_confirmar_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("✅ Confirmando...")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})
    _, paso = query.data.split("CONFIRMAR_", 1) if "CONFIRMAR_" in query.data else ("CONFIRMAR", registro.get("PASO_ACTUAL", ""))

    # 🟢 Si se confirma la observación → mostrar resumen limpio
    if paso == "OBS":
        logger.info("✅ [CONFIRMAR_OBS] Confirmando observación y mostrando resumen final")

        # 🧹 Eliminar mensaje del menú anterior
        old_menu_id = registro.pop("ULTIMO_MENSAJE_MENU", None)
        if old_menu_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=old_menu_id)
            except Exception:
                pass

        # 🧹 Eliminar también el mensaje anterior de confirmación de observación (si existe)
        try:
            await query.delete_message()
        except Exception:
            pass

        # ✅ Mostrar confirmación única
        await context.bot.send_message(
            chat_id=chat_id,
            text="✅ Observación seleccionada correctamente.",
            parse_mode="Markdown"
        )

        # 🧾 Luego mostrar resumen final
        await mostrar_resumen_final(update, context)
        return "RESUMEN_FINAL"

    # ============================================================
    # 🟢 1) CORRECCIÓN DESDE RESUMEN FINAL
    # ============================================================
    if registro.get("CORRECCION_ORIGEN") == "RESUMEN":
        tipo_paso = PASOS.get(paso, {}).get("tipo", "")
        if tipo_paso == "foto":
            msg = "📸 *Foto corregida correctamente.*"
        else:
            msg = "✅ *Campo corregido correctamente.*"

        try:
            await query.edit_message_text(msg, parse_mode="Markdown")
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")

        registro["CORRECCION_ORIGEN"] = None
        registro["VOLVER_A_RESUMEN"] = False
        registro["EN_CORRECCION"] = False
        registro["PASO_ACTUAL"] = "RESUMEN_FINAL"

        await mostrar_resumen_final(update, context)
        return "RESUMEN_FINAL"

    # ============================================================
    # 🟡 2) FLUJO REGULAR (captura normal de datos)
    # ============================================================
    tipo = PASOS.get(paso, {}).get("tipo")
    siguiente = PASOS.get(paso, {}).get("siguiente")

    # 📸 Si es foto (FOTO_CAJA, FOTO_CAJA_ABIERTA o FOTO_MEDICION)
    if tipo == "foto":
        try:
            await query.edit_message_text("✅ Foto subida correctamente.", parse_mode="Markdown")
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text="✅ Foto subida correctamente.", parse_mode="Markdown")

        if siguiente and siguiente != "OBS":
            registro["PASO_ACTUAL"] = siguiente
            instruccion = PASOS.get(siguiente, {}).get(
                "instruccion",
                PASOS.get(siguiente, {}).get("mensaje", f"➡️ Continúa con *{siguiente.replace('_',' ')}*")
            )
            await context.bot.send_message(chat_id=chat_id, text=instruccion, parse_mode="Markdown")
            return siguiente

        # Si siguiente es OBS → abrir menú de observaciones
        if siguiente == "OBS":
            registro["PASO_ACTUAL"] = "OBS_TIPO"

            # 🧹 Limpiar mensajes anteriores
            await limpiar_mensaje_anterior(context, chat_id, registro)

            msg = await context.bot.send_message(
                chat_id=chat_id,
                text="📋 Usa el menú para elegir el tipo de observación:",
                parse_mode="Markdown"
            )
            registro["ULTIMO_MENSAJE_MENU"] = msg.message_id

            await mostrar_menu_obs(chat_id, context, tipo=registro.get("OBS_TIPO") or None)
            return "OBS_TIPO"

    # ✏️ Si es texto o ubicación
    elif tipo in ("texto", "ubicacion"):
        if siguiente:
            registro["PASO_ACTUAL"] = siguiente
            instruccion = PASOS.get(siguiente, {}).get(
                "instruccion",
                PASOS.get(siguiente, {}).get("mensaje", f"➡️ Continúa con *{siguiente.replace('_',' ')}*")
            )
            await context.bot.send_message(chat_id=chat_id, text=instruccion, parse_mode="Markdown")
            return siguiente

    # ============================================================
    # 🔚 Si no hay más pasos, ir al menú de observaciones
    # ============================================================
    registro["PASO_ACTUAL"] = "OBS_TIPO"
    await limpiar_mensaje_anterior(context, chat_id, registro)
    msg = await context.bot.send_message(chat_id=chat_id, text="📋 Usa el menú para elegir el tipo de observación:", parse_mode="Markdown")
    registro["ULTIMO_MENSAJE_MENU"] = msg.message_id
    await mostrar_menu_obs(chat_id, context, tipo=None)
    return "OBS_TIPO"


# ============================================================
# ✏️ CORREGIR_<PASO> → puede venir de flujo normal o del resumen
# ============================================================
async def manejar_corregir_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("✏️ Corrigiendo...")
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})
    _, paso = query.data.split("CORREGIR_", 1) if "CORREGIR_" in query.data else ("CORREGIR", registro.get("PASO_ACTUAL", ""))

    # ⚙️ si NO venimos de resumen, es corrección dentro del flujo normal

     # ✅ Si venimos del resumen, marcamos el origen para volver luego
    if registro.get("VOLVER_A_RESUMEN", False):
        registro["CORRECCION_ORIGEN"] = "RESUMEN"
    if not registro.get("VOLVER_A_RESUMEN", False):
        registro["EN_CORRECCION"] = True   # ← para que al confirmar avance al siguiente paso
    registro["PASO_ACTUAL"] = paso

    tipo = PASOS.get(paso, {}).get("tipo", "texto")

    # caso especial: OBS → abre menú
    if paso == "OBS":
        registro["PASO_ACTUAL"] = "OBS_TIPO"
        await context.bot.send_message(
            chat_id=chat_id,
            text="🧭 Corrige la *observación* seleccionando nuevamente el tipo de elemento:",
            parse_mode="Markdown",
        )
        await mostrar_menu_obs(chat_id, context, tipo=None)
        return "OBS_TIPO"

    # mensajes por tipo
    mensajes = {
        "texto": f"✏️ Ingresa nuevamente el *{paso.replace('_', ' ')}*: ",
        "foto": "📸 Envía nuevamente la *foto solicitada*: ",
        "ubicacion": "📍 Envía nuevamente la *ubicación GPS* de la CTO/NAP/FAT: ",
    }

    # ✅ corregido el error de f-string
    mensaje_default = f"✏️ Ingresa nuevamente *{paso.replace('_', ' ')}*:"
    texto = f"{mensajes.get(tipo, mensaje_default)}\n\n🔁 Después confirma para continuar."

    await context.bot.send_message(
        chat_id=chat_id,
        text=texto,
        parse_mode="Markdown",
    )
    return paso


# ============================================================
# ✏️ MANEJAR CORRECCIONES DESDE EL RESUMEN FINAL
# ============================================================
async def manejar_edicion_desde_resumen_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})
    await query.answer("✏️ Corrigiendo campo...")

    # 🧹 Limpiar botones del mensaje anterior
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    # 🔍 Identificar el campo a corregir
    data = query.data
    if data.startswith("EDITAR_"):
        paso = data.replace("EDITAR_", "")
    elif data.startswith("CORREGIR_"):
        paso = data.replace("CORREGIR_", "")
    else:
        paso = registro.get("PASO_ACTUAL", "")

    logger.info(f"✏️ [RESUMEN_FINAL] Iniciando corrección del campo: {paso}")

    # 🧭 Marcar banderas de corrección
    registro["CORRECCION_ORIGEN"] = "RESUMEN"
    registro["VOLVER_A_RESUMEN"] = True
    registro["EN_CORRECCION"] = True
    registro["PASO_ACTUAL"] = paso

    # ============================================================
    # 🟡 CASO ESPECIAL: Observación → mostrar menú automático
    # ============================================================
    if paso in ("OBS", "OBS_TIPO", "OBS_SELECCION"):
        registro["PASO_ACTUAL"] = "OBS_TIPO"
        registro["CORRECCION_ORIGEN"] = "RESUMEN"
        registro["VOLVER_A_RESUMEN"] = True
        registro["EN_CORRECCION"] = True

        # 💬 Mostrar inmediatamente el menú CTO/NAP/FAT
        await context.bot.send_message(
            chat_id=chat_id,
            text="📋 Usa el menú para elegir el tipo de observación:",
            parse_mode="Markdown"
        )
        await mostrar_menu_obs(chat_id, context, tipo=None)
        logger.info("🟢 [RESUMEN_FINAL] Menú de observaciones desplegado automáticamente")
        registro["PASO_ACTUAL"] = "OBS_TIPO"
        return "OBS_TIPO"

    # ============================================================
    # 🔹 Otros campos → pedir nuevo valor normalmente
    # ============================================================

    # 🖼️ Si el paso es una de las nuevas fotos, mostrar mensaje específico
    if paso in ("FOTO_CAJA_ABIERTA", "FOTO_MEDICION"):
        texto = f"📸 Envía nuevamente la *{paso.replace('_',' ').title()}*."
        try:
            await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"❌ Error mostrando instrucción de corrección ({paso}): {e}")
        return paso


    tipo = PASOS.get(paso, {}).get("tipo", "texto")
    instruccion = PASOS.get(paso, {}).get(
        "instruccion", f"✏️ Envía el nuevo valor para *{paso.replace('_',' ')}*:"
    )

    if tipo == "foto":
        texto = f"📸 Envía nuevamente la *foto de {paso.replace('_',' ')}*."
    elif tipo == "ubicacion":
        texto = "📍 Envía la *nueva ubicación (GPS)* de la CTO/NAP/FAT."
    elif tipo == "texto":
        texto = f"✏️ Envía el nuevo *{paso.replace('_',' ')}*."
    else:
        texto = instruccion

    try:
        await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"❌ Error mostrando instrucción de corrección ({paso}): {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"✏️ Envía el nuevo valor para {paso}.")

    # 🔁 Retornar el mismo estado que se corrige
    logger.info(f"✏️ [RESUMEN_FINAL] Esperando nueva entrada para el paso: {paso}")
    return paso


# ============================================================
# 📋 MENÚ DE OBSERVACIONES (CTO / NAP / FAT) — versión final limpia y estable
# ============================================================
async def mostrar_menu_obs(chat_id, context, tipo: str | None = None, query=None):
    registro = context.user_data.setdefault("registro", {})
    es_flotante = query is not None

    # 🔹 Menú principal
    if not tipo or tipo in ("None", "", None):
        keyboard = [
            [InlineKeyboardButton("🟧 CTO", callback_data="OBS_TIPO_CTO")],
            [InlineKeyboardButton("🟦 NAP", callback_data="OBS_TIPO_NAP")],
            [InlineKeyboardButton("🟩 FAT", callback_data="OBS_TIPO_FAT")],
        ]
        texto = "🧩 *Selecciona el tipo de elemento* para registrar la observación:"
        markup = InlineKeyboardMarkup(keyboard)

        old_msg_id = registro.pop("ULTIMO_MENSAJE_OBS", None)
        if old_msg_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
            except Exception:
                pass

        msg = (await query.edit_message_text(texto, reply_markup=markup, parse_mode="Markdown")
               if es_flotante else
               await context.bot.send_message(chat_id=chat_id, text=texto, reply_markup=markup, parse_mode="Markdown"))

        registro["ULTIMO_MENSAJE_OBS"] = msg.message_id
        registro["PASO_ACTUAL"] = "OBS_TIPO"
        return "OBS_TIPO"

    # 🔹 Submenú CTO / NAP / FAT
    opciones = OBS_OPCIONES.get(tipo, [])
    if not opciones:
        texto = f"⚠️ No hay observaciones definidas para *{tipo}*."
        try:
        # ✅ Enviamos siempre un nuevo mensaje (ya no editamos el anterior)
            await context.bot.send_message(
                chat_id=chat_id,
                text=texto,
                reply_markup=markup,
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
        except Exception as e:
            logger.error(f"❌ Error mostrando submenú {tipo}: {e}")

        return "OBS_TIPO"

    keyboard = [[InlineKeyboardButton(obs, callback_data=f"OBS_SET_{idx}")]
                for idx, obs in enumerate(opciones)]
    keyboard.append([InlineKeyboardButton("🔙 Volver", callback_data="OBS_BACK")])

    texto = f"📝 *Selecciona la observación correspondiente a {tipo}:*"
    markup = InlineKeyboardMarkup(keyboard)

    old_msg_id = registro.pop("ULTIMO_MENSAJE_OBS", None)
    if old_msg_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
        except Exception:
            pass

    # ✅ Enviamos siempre un nuevo mensaje (ya no editamos el anterior)
    msg = await context.bot.send_message(
        chat_id=chat_id,
        text=texto,
        reply_markup=markup,
        parse_mode="Markdown",
        disable_web_page_preview=True
    )

    registro["ULTIMO_MENSAJE_OBS"] = msg.message_id
    registro["PASO_ACTUAL"] = "OBS_SELECCION"
    return "OBS_SELECCION"


# ================== RESUMEN FINAL (versión mejorada y sincronizada) ==================
async def mostrar_resumen_final(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        reg = context.user_data.get("registro", {})  # <- SIEMPRE dict
        chat_id = update.effective_chat.id
        bot = context.bot

        # 🧹 Eliminar mensaje anterior si ya existía un resumen previo
        old_msg_id = reg.pop("ULTIMO_MENSAJE_RESUMEN", None)
        if old_msg_id:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
                logger.info("🧹 Mensaje viejo de resumen eliminado correctamente.")
            except Exception:
                logger.debug("⚠️ No se pudo eliminar el mensaje anterior (ya borrado o editado).")

        # Valores seguros
        ticket       = reg.get("TICKET", "-")
        dni          = reg.get("DNI", "-")
        cliente      = reg.get("NOMBRE_CLIENTE", "-")
        cuadrilla    = reg.get("CUADRILLA", "-")
        partner      = reg.get("PARTNER", "-")
        nodo         = reg.get("NODO", "-")
        cod_caja     = reg.get("CODIGO_CAJA", "-")
        lat          = reg.get("LAT_CAJA")
        lng          = reg.get("LNG_CAJA")
        dep          = reg.get("DEPARTAMENTO", "-")
        prov         = reg.get("PROVINCIA", "-")
        dist         = reg.get("DISTRITO", "-")
        observacion  = reg.get("OBSERVACION", reg.get("OBS", "-"))

        # Coordenadas visibles
        coord_txt = f"({lat}, {lng})" if (lat is not None and lng is not None) else "(-, -)"
        link_mapa = f"https://maps.google.com/?q={lat},{lng}" if (lat and lng) else None

        # 📋 Texto del resumen
        resumen = (
            "📋 *Resumen de la incidencia*\n\n"
            f"🎫 *Ticket:* `{ticket}`\n"
            f"🪪 *DNI:* {dni}\n"
            f"👤 *Cliente:* {cliente}\n"
            f"👷 *Cuadrilla:* {cuadrilla}\n"
            f"🏢 *Partner:* {partner}\n"
            f"🏷 *Código CTO/NAP/FAT:* {cod_caja}\n"
            f"📡 *Nodo:* {nodo}\n"
            f"📍 *Coordenadas:* {coord_txt}\n"
            f"🧭 *Lugar:* {prov}, {dep}, {dist}\n"
        )
        if link_mapa:
            resumen += f"[🌐 Ver ubicación CTO/NAP/FAT]({link_mapa})\n"

        foto_ok = "✅" if reg.get("FOTO_CAJA") else "❌"
        foto_open_ok = "✅" if reg.get("FOTO_CAJA_ABIERTA") else "❌"
        foto_med_ok = "✅" if reg.get("FOTO_MEDICION") else "❌"

        resumen += f"📸 *Foto CTO/NAP/FAT (Exterior):* {foto_ok}\n"
        resumen += f"📸 *Foto CTO/NAP/FAT (Interior):* {foto_open_ok}\n"
        resumen += f"📸 *Foto CTO/NAP/FAT (Medición):* {foto_med_ok}\n"
        resumen += f"📝 *Observaciones:* {observacion}\n\n"
        resumen += "¿Deseas confirmar tu registro?"

        # 🔘 Botonera
        keyboard = [
            [InlineKeyboardButton("✅ Guardar", callback_data="FINAL_GUARDAR")],
            [InlineKeyboardButton("✏️ Corregir", callback_data="FINAL_CORREGIR")],
            [InlineKeyboardButton("❌ Cancelar", callback_data="FINAL_CANCELAR")],
        ]

        markup = InlineKeyboardMarkup(keyboard)

        # 📤 Mostrar resumen: callback o mensaje directo (con fallback seguro)
        if getattr(update, "callback_query", None):
            try:
                msg = await update.callback_query.edit_message_text(
                    resumen,
                    parse_mode="Markdown",
                    reply_markup=markup,
                    disable_web_page_preview=True,
                )
            except Exception as e:
                logger.warning(f"⚠️ No se pudo editar mensaje previo: {e} → enviando nuevo mensaje.")
                msg = await bot.send_message(
                    chat_id=chat_id,
                    text=resumen,
                    parse_mode="Markdown",
                    reply_markup=markup,
                    disable_web_page_preview=True,
                )
        else:
            msg = await update.message.reply_text(
                resumen,
                parse_mode="Markdown",
                reply_markup=markup,
                disable_web_page_preview=True,
            )

        # 🧠 Guardar el nuevo mensaje y estado
        reg["ULTIMO_MENSAJE_RESUMEN"] = msg.message_id
        reg["PASO_ACTUAL"] = "RESUMEN_FINAL"

        logger.info("🧾 Resumen final mostrado correctamente.")
        return "RESUMEN_FINAL"

    except Exception as e:
        logger.error(f"❌ Error en mostrar_resumen_final: {e}")
        try:
            await context.bot.send_message(update.effective_chat.id, f"⚠️ Error mostrando resumen: {e}")
        except Exception:
            pass
        return ConversationHandler.END


# ============================================================
# 📋 CALLBACK: Acciones dentro del RESUMEN FINAL (versión limpia y estable)
# ============================================================
async def resumen_final_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    accion = query.data
    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})

    # 🔹 Limpieza básica de botones
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except Exception:
        pass

    # ============================================================
    # 💾 GUARDAR REGISTRO
    # ============================================================
    if accion == "FINAL_GUARDAR":
        await query.answer("💾 Guardando...")
        logger.info("💾 [RESUMEN_FINAL] Guardando registro desde resumen final")
        return await guardar_registro(update, context)

    # ============================================================
    # ✏️ CORREGIR (abrir menú de correcciones limpio)
    # ============================================================
    if accion == "FINAL_CORREGIR":
        await query.answer("✏️ Elige un campo a corregir")

        texto = "✏️ *Selecciona el campo que deseas corregir:*"
        keyboard = [
            [InlineKeyboardButton("🎫 Ticket", callback_data="EDITAR_TICKET")],
            [InlineKeyboardButton("🏷 Código CTO/NAP/FAT", callback_data="EDITAR_CODIGO_CAJA")],
            [InlineKeyboardButton("📍 Ubicación CTO/NAP/FAT", callback_data="EDITAR_UBICACION_CTO")],
            [InlineKeyboardButton("📸 Foto CTO/NAP/FAT (Exterior)", callback_data="EDITAR_FOTO_CAJA")],
            [InlineKeyboardButton("📸 Foto CTO/NAP/FAT (Interior)", callback_data="EDITAR_FOTO_CAJA_ABIERTA")],
            [InlineKeyboardButton("📸 Foto CTO/NAP/FAT (Medición)", callback_data="EDITAR_FOTO_MEDICION")],
            [InlineKeyboardButton("📝 Observación", callback_data="EDITAR_OBS")],
        ]

        await context.bot.send_message(
            chat_id=chat_id,
            text=texto,
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

        registro["VOLVER_A_RESUMEN"] = True
        registro["EN_CORRECCION"] = True
        registro["PASO_ACTUAL"] = "CORREGIR"
        return "CORREGIR"

    # ============================================================
    # ❌ CANCELAR REGISTRO
    # ============================================================
    if accion == "FINAL_CANCELAR":
        await query.answer("❌ Cancelado")
        try:
            await query.edit_message_text("❌ Registro cancelado por el usuario.")
        except Exception:
            await context.bot.send_message(chat_id=chat_id, text="❌ Registro cancelado por el usuario.")
        context.user_data.pop("registro", None)
        return ConversationHandler.END

    # ============================================================
    # 🚫 Cualquier otra acción desconocida
    # ============================================================
    await query.answer("⚠️ Acción no reconocida.")
    logger.warning(f"⚠️ Acción desconocida en resumen_final_callback: {accion}")
    return "RESUMEN_FINAL"

# ============================================================
# 🔙 CALLBACK: VOLVER DESDE MENÚ DE CORRECCIONES → RESUMEN FINAL
# ============================================================
async def manejar_volver_desde_resumen_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})

    await query.answer("↩️ Volviendo al resumen final...")

    # 🧹 Limpieza visual (elimina botones previos)
    try:
        await query.edit_message_text("🔄 Volviendo al resumen final...", parse_mode="Markdown")
    except Exception:
        pass

    # 🧠 Restaurar contexto
    registro["PASO_ACTUAL"] = "RESUMEN_FINAL"
    registro["EN_CORRECCION"] = False
    registro["VOLVER_A_RESUMEN"] = False

    logger.info("🔙 [VOLVER] Regresando correctamente al Resumen Final")

    # ✅ Mostrar nuevamente el resumen
    try:
        await mostrar_resumen_final(update, context)
    except Exception as e:
        logger.error(f"❌ Error mostrando resumen: {e}")
        await context.bot.send_message(chat_id, "⚠️ No se pudo mostrar el resumen final, intenta nuevamente.")

    return "RESUMEN_FINAL"


# ============================================================
# 🧭 CALLBACK: manejar_tipo_obs_callback (versión robusta con BACK funcional)
# ============================================================
async def manejar_tipo_obs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})

    logger.info(f"🟢 [OBS_TIPO] Callback recibido: {data}")

    # 🔙 Volver al menú principal CTO/NAP/FAT
    if data in ("OBS_TIPO_BACK", "OBS_BACK"):
        logger.info("🔙 [OBS_TIPO] Volviendo al menú principal CTO/NAP/FAT")

        # 🔹 Limpieza de botones previos
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await asyncio.sleep(0.3)

        # 🔹 Intentar mostrar el menú principal
        try:
            await mostrar_menu_obs(chat_id, context, tipo=None)
            logger.info("📋 Menú principal CTO/NAP/FAT mostrado correctamente.")
        except Exception as e:
            logger.error(f"❌ Error al volver al menú principal: {e}")
            await context.bot.send_message(chat_id=chat_id, text="⚠️ No se pudo mostrar el menú principal de observaciones. Intenta nuevamente.", parse_mode="Markdown",)

        registro["PASO_ACTUAL"] = "OBS_TIPO"
        return "OBS_TIPO"


    # 🔸 Selección de tipo CTO/NAP/FAT
    if data.startswith("OBS_TIPO_"):
        tipo = data.replace("OBS_TIPO_", "")
        registro["OBS_TIPO"] = tipo
        logger.info(f"✅ [OBS_TIPO] Tipo de observación seleccionado: {tipo}")
        await mostrar_menu_obs(chat_id, context, tipo=tipo, query=query)
        registro["PASO_ACTUAL"] = "OBS_SELECCION"
        return "OBS_SELECCION"

    # 🧩 Seguridad extra: si algo no coincide, mantenemos OBS_TIPO activo
    return registro.get("PASO_ACTUAL", "OBS_TIPO")


# ============================================================
# 📝 CALLBACK: Manejar selección de observación específica (versión limpia sin botón extra)
# ============================================================
async def manejar_observacion_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # 🧹 Eliminar mensaje del menú anterior (para que no quede flotando)
    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})
    old_menu_id = registro.pop("ULTIMO_MENSAJE_MENU", None)
    if old_menu_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=old_menu_id)
        except Exception:
            pass

    data = query.data
    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})

    logger.info(f"🟢 [OBS_SET] Callback recibido: {data}")

    tipo_actual = registro.get("OBS_TIPO", "CTO")
    opciones = OBS_OPCIONES.get(tipo_actual, [])
    try:
        idx = int(data.replace("OBS_SET_", ""))
        observacion = opciones[idx] if idx < len(opciones) else None
    except Exception:
        observacion = None

    if not observacion:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ No se pudo identificar la observación seleccionada. Intenta nuevamente.",
            parse_mode="Markdown",
        )
        return "OBS_TIPO"

    # 🧾 Guardar observación
    registro["OBSERVACION"] = observacion
    registro["PASO_ACTUAL"] = "OBS_CONFIRMAR"

    # ✅ Mostrar confirmación y botones
    texto = f"✅ *Observación registrada:* {observacion}\n\n¿Deseas confirmar o corregir?"
    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Confirmar", callback_data="CONFIRMAR_OBS"),
            InlineKeyboardButton("✏️ Corregir", callback_data="CORREGIR_OBS")
        ]
    ])

    try:
        await query.edit_message_text(text=texto, parse_mode="Markdown", reply_markup=markup)
    except Exception as e:
        logger.error(f"❌ Error mostrando botones de confirmación OBS: {e}")
        await context.bot.send_message(chat_id=chat_id, text=texto, parse_mode="Markdown", reply_markup=markup)

    return "CONFIRMAR"


# ============================================================
# 📋 CALLBACK: Ir directamente al RESUMEN FINAL después de OBS
# ============================================================
async def manejar_ir_resumen_final_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("📄 Mostrando resumen final...")

    chat_id = query.message.chat_id
    registro = context.user_data.setdefault("registro", {})
    registro["PASO_ACTUAL"] = "RESUMEN_FINAL"
    registro["EN_CORRECCION"] = False
    registro["VOLVER_A_RESUMEN"] = False

    try:
        await mostrar_resumen_final(update, context)
    except Exception as e:
        logger.error(f"❌ Error mostrando resumen desde OBS: {e}")
        await context.bot.send_message(chat_id, "⚠️ No se pudo mostrar el resumen final.")

    return "RESUMEN_FINAL"


# ============= GUARDAR REGISTRO ====================
async def guardar_registro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Guarda el registro del técnico en OneDrive (Excel) y refleja el resultado en Google Sheets.
    Maneja errores de red, bloqueos y duplicados con tolerancia total.
    """
    try:
        registro = context.user_data.get("registro", {})  # ✅ ahora se llama igual que en el resto del flujo
        chat_id = update.effective_chat.id

        if not registro:
            await context.bot.send_message(update.effective_chat.id, "⚠️ No hay datos de registro activos.")
            return ConversationHandler.END

        # 🧹 Eliminar mensaje del resumen anterior (para que no quede duplicado)
        old_msg_id = registro.pop("ULTIMO_MENSAJE_RESUMEN", None)
        if old_msg_id:
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=old_msg_id)
                logger.info("🧹 Resumen de incidencia eliminado antes de mostrar el mensaje final.")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo eliminar el mensaje del resumen final: {e}")

        # 🧑‍💻 Datos del usuario
        user = update.effective_user
        registro["USER_ID"] = user.id
        registro["USUARIO"] = user.full_name or "-"
        registro["USERNAME"] = user.username or "-"

        # 🕒 Fecha y hora actual
        fecha, hora = get_fecha_hora()
        registro["FECHA"] = fecha
        registro["HORA"] = hora

        # 🌍 Geocodificación si falta
        if not registro.get("DEPARTAMENTO") or not registro.get("PROVINCIA") or not registro.get("DISTRITO"):
            lat, lng = registro.get("LAT_CAJA"), registro.get("LNG_CAJA")
            if lat and lng:
                dep, prov, dist = geocodificar(lat, lng)
                if dep != "-" or prov != "-" or dist != "-":
                    registro["DEPARTAMENTO"] = dep
                    registro["PROVINCIA"] = prov
                    registro["DISTRITO"] = dist

        # 🔹 Normalización de datos
        nodo_val = registro.get("NODO", "-")
        foto_val = registro.get("FOTO_CAJA", "")
        foto_bytes = registro.get("FOTO_CAJA_BYTES")

        # 📝 Corregimos observación (si viene de menú)
        registro["OBS"] = registro.get("OBSERVACION", registro.get("OBS", "-"))

        # 🧾 Fila completa (coincide con tus encabezados)
        fila = [
            registro.get("USER_ID", ""),
            registro.get("FECHA", ""),
            registro.get("HORA", ""),
            registro.get("PARTNER", "-"),
            registro.get("CUADRILLA", "-"),
            registro.get("TICKET", ""),
            registro.get("DNI", "-"),
            registro.get("NOMBRE_CLIENTE", "-"),
            registro.get("NODO", "-"),
            registro.get("CODIGO_CAJA", ""),
            registro.get("FOTO_CAJA", ""),
            registro.get("FOTO_CAJA_ABIERTA"),
            registro.get("FOTO_MEDICION"),
            registro.get("LAT_CAJA", ""),
            registro.get("LNG_CAJA", ""),
            registro.get("DEPARTAMENTO", ""),
            registro.get("PROVINCIA", ""),
            registro.get("DISTRITO", ""),
            registro.get("OBS", "-"),
        ]

        # ==========================================
        # ☁️ Guardar registro en OneDrive + Google Sheets (mensaje flotante)
        # ==========================================
        msg_guardando = await context.bot.send_message(
            update.effective_chat.id,
            "💾 Guardando registro..."
        )

        try:
            ok = append_row_online(fila)  # OneDrive
            if not ok:
                logger.warning("🟡 No se pudo escribir en OneDrive. Reflejando solo en Google Sheets.")
            else:
                logger.info("✅ Registro guardado correctamente en OneDrive.")

            gs_append_row(fila)  # Google Sheets
            logger.info("☁️ Fila reflejada correctamente en Google Sheets.")
        except Exception as e:
            logger.error(f"❌ Error guardando (OneDrive/Sheets): {e}")

        finally:
            # 🧹 Mensaje flotante: lo eliminamos para no dejarlo en el chat
            try:
                await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=msg_guardando.message_id)
            except Exception:
                pass
        # ==========================================
        # 📋 Resumen visual (enviado al técnico y supervisión)
        # ==========================================
        lat = registro.get("LAT_CAJA", "-")
        lng = registro.get("LNG_CAJA", "-")
        dep = registro.get("DEPARTAMENTO", "-")
        prov = registro.get("PROVINCIA", "-")
        dist = registro.get("DISTRITO", "-")
        link_mapa = f"https://maps.google.com/?q={lat},{lng}"

        resumen_final = (
            "✅ *Registro guardado exitosamente*\n\n"
            f"🎫 *Ticket:* `{registro.get('TICKET', '-')}`\n"
            f"🪪 *DNI:* {registro.get('DNI', '-')}\n"
            f"👤 *Cliente:* {registro.get('NOMBRE_CLIENTE', '-')}\n"
            f"👷 *Cuadrilla:* {registro.get('CUADRILLA', '-')}\n"
            f"🏢 *Partner:* {registro.get('PARTNER', '-')}\n"
            f"🏷 *Código CTO/NAP/FAT:* {registro.get('CODIGO_CAJA', '-')}\n"
            f"📡 *Nodo:* {registro.get('NODO', '-')}\n"
            f"📍 *Coordenadas:* ({lat}, {lng})\n"
            f"🧭 *Ubicación:* {prov}, {dep}, {dist}\n"
            f"[🌐 Ver ubicación CTO]({link_mapa})\n"
            f"📸 *Foto CTO/NAP/FAT (Exterior):* ✅\n"
            f"📸 *Foto CTO/NAP/FAT (Interior):* ✅\n"
            f"📸 *Foto CTO/NAP/FAT (Medición):* ✅\n"          
            f"📝 *Observaciones:* {registro.get('OBS', '-')}"
        )

        # 📲 Enviar al técnico
        msg_final = await context.bot.send_message(chat_id, resumen_final, parse_mode="Markdown")
        registro["ULTIMO_MENSAJE_RESUMEN"] = msg_final.message_id  # opcional, por si se usa luego

        # 📢 Enviar al grupo de supervisión (con foto)
        for grupo_id in GRUPO_SUPERVISION_ID:
            try:
                await context.bot.send_message(chat_id=grupo_id, text=resumen_final, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"❌ Error enviando al grupo {grupo_id}: {e}")

        # 🧹 Limpiar datos del usuario
        context.user_data.pop("registro", None)
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"❌ Error general en guardar_registro: {e}")
        await context.bot.send_message(
            update.effective_chat.id,
            "⚠️ Ocurrió un error al guardar. Contacta a soporte."
        )
        return ConversationHandler.END


# ================== CANCEL ==================
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    if chat_id in GRUPO_SUPERVISION_ID:
        return ConversationHandler.END

    context.user_data.pop("registro", None)
    await update.message.reply_text("❌ Registro cancelado.", reply_markup=ReplyKeyboardRemove())
    return ConversationHandler.END


# ================== FUNCIONES AUXILIARES ==================
async def limpiar_mensaje_anterior(context, chat_id, registro, clave="ULTIMO_MENSAJE_MENU"):
    """
    Elimina el último mensaje auxiliar (como menús o instrucciones repetidas)
    guardado en registro[clave].
    """
    old_id = registro.pop(clave, None)
    if old_id:
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=old_id)
        except Exception:
            pass



# ============================
# 🔁 POLLING SEGURO CON REINTENTOS
# ============================
async def safe_polling(app):
    """
    Ejecuta el polling con reintentos progresivos si se cae la conexión.
    """
    intento = 1
    while True:
        try:
            logger.info("🚀 Iniciando run_polling()...")
            await app.run_polling(allowed_updates=Update.ALL_TYPES)
        except NetworkError as e:
            espera = min(60, 15 * intento)
            logger.warning(f"🌐 Error de red: {e}. Reintentando en {espera}s...")
            await asyncio.sleep(espera)
            intento += 1
        except Exception as e:
            logger.error(f"💥 Error inesperado en safe_polling: {e}")
            await asyncio.sleep(10)



def escape_markdown(text: str) -> str:
    """Evita errores de formato en MarkdownV2."""
    return re.sub(r'([_\*\[\]\(\)~`>\#\+\-=|{}\.!])', r'\\\1', str(text))

# ================== MAIN ==================
def main():
    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # ==========================
    # 🔁 CONVERSATION HANDLER
    # ==========================
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("start", start),
            CommandHandler("registro", comando_registro),
        ],
        states={
            # ====== PASO 1: TICKET ======
            "TICKET": [
                MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: manejar_paso(u, c, "TICKET")),
            ],

            # ====== PASO 2: CÓDIGO CTO/NAP/FAT ======
            "CODIGO_CAJA": [
                MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: manejar_paso(u, c, "CODIGO_CAJA")),
            ],

            # ====== PASO 3: UBICACIÓN ======
            "UBICACION_CTO": [
                MessageHandler(filters.LOCATION, lambda u, c: manejar_paso(u, c, "UBICACION_CTO")),
            ],

            # ====== PASO 4: FOTO ======
            "FOTO_CAJA": [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, lambda u, c: manejar_paso(u, c, "FOTO_CAJA")),
            ],
            "FOTO_CAJA_ABIERTA": [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, lambda u, c: manejar_paso(u, c, "FOTO_CAJA_ABIERTA")),
            ],
            "FOTO_MEDICION": [
                MessageHandler(filters.PHOTO | filters.Document.IMAGE, lambda u, c: manejar_paso(u, c, "FOTO_MEDICION")),
            ],

            # ====== PASO 5: OBSERVACIONES ======
            "OBS": [
                MessageHandler(filters.TEXT & ~filters.COMMAND, lambda u, c: manejar_paso(u, c, "OBS")),
            ],

            # 👇 Submenús de observaciones
            "OBS_TIPO": [
                CallbackQueryHandler(manejar_tipo_obs_callback, pattern=r"^OBS_TIPO_.*$"),
                CallbackQueryHandler(manejar_tipo_obs_callback, pattern=r"^OBS_TIPO_BACK$"),
            ],
            "OBS_SELECCION": [
                CallbackQueryHandler(manejar_observacion_callback, pattern=r"^OBS_SET_.*$"),
                CallbackQueryHandler(manejar_tipo_obs_callback, pattern=r"^OBS_BACK$"),
            ],

            # ====== CONFIRMAR / CORREGIR / EDITAR ======
            "CONFIRMAR": [
                CallbackQueryHandler(manejar_confirmar_callback, pattern=r"^CONFIRMAR_.*$"),
                CallbackQueryHandler(manejar_corregir_callback, pattern=r"^CORREGIR_.*$"),
                CallbackQueryHandler(manejar_ir_resumen_final_callback, pattern=r"^IR_RESUMEN_FINAL$"),
                CallbackQueryHandler(manejar_edicion_desde_resumen_callback, pattern=r"^EDITAR_.*$"),
            ],
            # ====== CORREGIR (cuando viene desde resumen final) ======
            "CORREGIR": [
                CallbackQueryHandler(manejar_edicion_desde_resumen_callback, pattern=r"^EDITAR_.*$"),
                MessageHandler(filters.ALL, lambda u, c: manejar_paso(u, c, c.user_data.get("registro", {}).get("PASO_ACTUAL", ""))),
            ],
            # ====== RESUMEN FINAL ======
            "RESUMEN_FINAL": [
                CallbackQueryHandler(resumen_final_callback, pattern=r"^FINAL_.*$"),
                CallbackQueryHandler(manejar_edicion_desde_resumen_callback, pattern=r"^EDITAR_.*$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
    )

    # ==========================
    # 🔁 JOBS Y HANDLERS EXTRA
    # ==========================
    app.add_handler(conv_handler)

    # 🕒 JOB SCHEDULER
    app.job_queue.run_repeating(_job_recarga_ordenes, interval=35 * 60, first=5)

    # ==========================
    # 🚀 INICIO DEL BOT
    # ==========================
    logger.info("🤖 Bot de Incidencias iniciado correctamente...")

    try:
        # ✅ Más seguro que asyncio.get_event_loop()
        import nest_asyncio
        nest_asyncio.apply()
        app.run_polling(allowed_updates=Update.ALL_TYPES)
    except KeyboardInterrupt:
        logger.warning("🛑 Bot detenido manualmente.")
    except Exception as e:
        logger.error(f"❌ Error crítico en main(): {e}")

# ==============================
# 🔎 CARGAS INICIALES
# ==============================
if __name__ == "__main__":
    cargar_ordenes_trabajo(force=True)
    verificar_carpeta_imagenes_inicial()
    ensure_excel_exists()
    cargar_cajas_nodos()
    main()
