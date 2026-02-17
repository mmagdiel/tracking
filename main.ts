/**
 * Webhook receptor para notificaciones Push Tracking de Coordinadora
 * Retransmite mensajes Pub/Sub a clientes WebSocket autenticados
 *
 * Uso: npm start
 */

import http from 'http';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import net from 'net';
import { Pool } from 'pg';

interface PubSubMessage {
    message?: {
        data: string;
    };
    [key: string]: unknown;
}

interface TrackingData {
    tracking_number?: string;
    referencia?: string;
    comment?: string;
    codigo?: number;
    codigo_cliente?: string;
    fecha?: string;
    hora?: string;
    anterior?: string;
    referencia_anterior?: string;
    nit_cliente?: string;
    div_cliente?: string;
    auxiliar?: string;
    vinculo_guia?: string;
    nombre_entrega?: string;
    [key: string]: unknown;
}

interface LogRegistro {
    fecha_recepcion: string;
    ip_origen: string | string[] | undefined;
    raw_body: string;
    data_decodificada: TrackingData | null;
    error: string | null;
}

type TokensClientes = Record<string, string>;

const PORT = process.env.PORT || 3000;
const CLIENT_ACCESS_TOKEN = 'gg8rY1c5Tcp1UhHqV0X2B5bSF4GcSKtwQerhLRQx4HhwhxHXiJPtRp3TDGVx';
const LOG_FILE = path.join(__dirname, 'tracking_log.json');

const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:mypassword@localhost:5432/postgres';
const pgPool = new Pool({
    connectionString: DATABASE_URL,
});

// Tokens por cliente: { cliente_id: token }
const TOKENS_CLIENTES: TokensClientes = {
    'cliente_1': CLIENT_ACCESS_TOKEN,
    // 'cliente_2': process.env.CLIENT_2_TOKEN,
    // Agrega más según necesites
};

// Limpiar tokens no definidos
Object.keys(TOKENS_CLIENTES).forEach(key => {
    if (!TOKENS_CLIENTES[key]) delete TOKENS_CLIENTES[key];
});

if (Object.keys(TOKENS_CLIENTES).length === 0) {
    console.error('ERROR: No hay tokens de clientes definidos');
    console.error('Define al menos una variable de entorno como CLIENT_1_TOKEN=tu_token');
    process.exit(1);
}

// Clientes WebSocket conectados: Map<socket, cliente_id>
const clientesWS: Map<net.Socket, string> = new Map();

// ================== UTILIDADES ==================

function validarToken(token: string | null): string | null {
    for (const [clienteId, clienteToken] of Object.entries(TOKENS_CLIENTES)) {
        if (clienteToken === token) {
            return clienteId;
        }
    }
    return null;
}

function leerLogs(): LogRegistro[] {
    try {
        if (fs.existsSync(LOG_FILE)) {
            return JSON.parse(fs.readFileSync(LOG_FILE, 'utf8'));
        }
    } catch (e) {
        console.error('Error leyendo logs:', (e as Error).message);
    }
    return [];
}

function guardarLogs(logs: LogRegistro[]): void {
    logs = logs.slice(0, 50);
    fs.writeFileSync(LOG_FILE, JSON.stringify(logs, null, 2));
}

function decodificarPubSub(rawBody: string): TrackingData | null {
    try {
        const mensaje: PubSubMessage = JSON.parse(rawBody);

        const data = mensaje.message?.data;
        if (typeof data === 'string' && data.length > 0) {
            const base64Normalizada = data
                .replace(/\s+/g, '')
                .replace(/-/g, '+')
                .replace(/_/g, '/');

            const padding = base64Normalizada.length % 4;
            const base64ConPadding = padding === 0
                ? base64Normalizada
                : base64Normalizada + '='.repeat(4 - padding);

            const dataDecodificada = Buffer.from(base64ConPadding, 'base64').toString('utf8');
            const dataSinBOM = dataDecodificada.replace(/^\uFEFF/, '');

            try {
                return JSON.parse(dataSinBOM);
            } catch {
                return { data: dataSinBOM } as unknown as TrackingData;
            }
        }

        return mensaje as TrackingData;
    } catch (e) {
        return null;
    }
}

function esDecodedFallback(data: TrackingData): boolean {
    return typeof (data as unknown as { data?: unknown }).data === 'string' && Object.keys(data).length === 1;
}

function normalizarHora(hora: string | undefined): string | null {
    if (!hora) return null;
    const trimmed = hora.trim();
    if (!trimmed) return null;
    // Aceptar formatos como "13: 51: 43.456818" o "10:01:04.987708"
    return trimmed.replace(/\s+/g, '');
}

async function obtenerTrackingCode(trackingNumber: string): Promise<string | null> {
    const query = 'SELECT "trackingCode" FROM orders WHERE tracking_number = $1 LIMIT 1';
    const result = await pgPool.query(query, [trackingNumber]);
    const value = result.rows[0]?.trackingCode;
    return typeof value === 'string' && value.trim() ? value.trim() : null;
}

async function insertarTracking(data: TrackingData, trackingCode: string | null): Promise<void> {
    const query = `
INSERT INTO tracking (
  "trackingNumber",
  "referencia",
  "comment",
  "codigo",
  "codigoCliente",
  "fecha",
  "hora",
  "anterior",
  "referenciaAnterior",
  "nitCliente",
  "divCliente",
  "auxiliar",
  "vinculoGuia",
  "nombreEntrega",
  "trackingCode"
) VALUES (
  $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
)`;

    const fecha = typeof data.fecha === 'string' && data.fecha.trim() ? data.fecha.trim() : null;
    const hora = normalizarHora(typeof data.hora === 'string' ? data.hora : undefined);

    await pgPool.query(query, [
        data.tracking_number ?? null,
        typeof data.referencia === 'string' ? data.referencia : null,
        typeof data.comment === 'string' ? data.comment : null,
        typeof data.codigo === 'number' ? data.codigo : null,
        typeof data.codigo_cliente === 'string' ? data.codigo_cliente : null,
        fecha,
        hora,
        typeof data.anterior === 'string' ? data.anterior : null,
        typeof data.referencia_anterior === 'string' ? data.referencia_anterior : null,
        typeof data.nit_cliente === 'string' ? data.nit_cliente : null,
        typeof data.div_cliente === 'string' ? data.div_cliente : null,
        typeof data.auxiliar === 'string' ? data.auxiliar : null,
        typeof data.vinculo_guia === 'string' ? data.vinculo_guia : null,
        typeof data.nombre_entrega === 'string' ? data.nombre_entrega : null,
        trackingCode,
    ]);
}

function procesarNotificacion(rawBody: string, req: http.IncomingMessage): LogRegistro {
    const registro: LogRegistro = {
        fecha_recepcion: new Date().toISOString(),
        ip_origen: req.headers['x-forwarded-for'] || req.socket.remoteAddress,
        raw_body: rawBody,
        data_decodificada: decodificarPubSub(rawBody),
        error: null
    };

    if (!registro.data_decodificada) {
        registro.error = 'No se pudo decodificar el mensaje';
    }

    const logs = leerLogs();
    logs.unshift(registro);
    guardarLogs(logs);

    return registro;
}

// ================== WEBSOCKET ==================

function calcularAcceptKey(key: string): string {
    const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
    return crypto.createHash('sha1').update(key + GUID).digest('base64');
}

function createWSFrame(message: string): Buffer {
    const payload = Buffer.from(message);
    const length = payload.length;

    let frame: Buffer;
    if (length < 126) {
        frame = Buffer.alloc(2 + length);
        frame[0] = 0x81;
        frame[1] = length;
        payload.copy(frame, 2);
    } else if (length < 65536) {
        frame = Buffer.alloc(4 + length);
        frame[0] = 0x81;
        frame[1] = 126;
        frame.writeUInt16BE(length, 2);
        payload.copy(frame, 4);
    } else {
        frame = Buffer.alloc(10 + length);
        frame[0] = 0x81;
        frame[1] = 127;
        frame.writeBigUInt64BE(BigInt(length), 2);
        payload.copy(frame, 10);
    }

    return frame;
}

function enviarACliente(socket: net.Socket, data: object): void {
    try {
        const frame = createWSFrame(JSON.stringify(data));
        socket.write(frame);
    } catch (e) {
        console.error('Error enviando a cliente:', (e as Error).message);
    }
}

function broadcast(data: object): void {
    clientesWS.forEach((clienteId, socket) => {
        enviarACliente(socket, data);
    });
}

function handleWebSocketUpgrade(req: http.IncomingMessage, socket: net.Socket): void {
    const url = new URL(req.url!, `http://${req.headers.host}`);
    const token = url.searchParams.get('token');

    // Validar token al conectar
    const clienteId = validarToken(token);

    if (!clienteId) {
        console.log(`[WS] Conexión rechazada - Token inválido`);
        socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
        socket.destroy();
        return;
    }

    console.log(`[WS] Cliente conectado: ${clienteId}`);

    // Completar handshake WebSocket
    const key = req.headers['sec-websocket-key']!;
    const acceptKey = calcularAcceptKey(key);

    socket.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${acceptKey}\r\n` +
        '\r\n'
    );

    // Registrar cliente
    clientesWS.set(socket, clienteId);

    // Enviar confirmación
    enviarACliente(socket, {
        tipo: 'conexion',
        mensaje: 'Conectado exitosamente',
        cliente: clienteId
    });

    socket.on('close', () => {
        console.log(`[WS] Cliente desconectado: ${clienteId}`);
        clientesWS.delete(socket);
    });

    socket.on('error', (err: Error) => {
        console.error(`[WS] Error con ${clienteId}:`, err.message);
        clientesWS.delete(socket);
    });
}

// ================== HTML MONITOR ==================

function generarHTML(host: string): string {
    const logs = leerLogs();
    const clientesConectados = Array.from(clientesWS.values());

    let notificacionesHTML = '';

    if (logs.length === 0) {
        notificacionesHTML = `
Aun no se han recibido notificaciones.
Esperando que Coordinadora envie datos a este endpoint...
`;
    } else {
        logs.forEach((log, index) => {
            let datosDecodificados = '  (No se pudo decodificar)\n';

            if (log.data_decodificada) {
                datosDecodificados = '';
                for (const [campo, valor] of Object.entries(log.data_decodificada)) {
                    datosDecodificados += `  ${campo}: ${valor}\n`;
                }
            }

            const rawTruncado = log.raw_body.length > 500
                ? log.raw_body.substring(0, 500) + '...(truncado)'
                : log.raw_body;

            notificacionesHTML += `
----------------------------------------
NOTIFICACION #${index + 1}
Fecha recepcion: ${log.fecha_recepcion}
IP origen: ${log.ip_origen}

DATOS DECODIFICADOS:
${datosDecodificados}
${log.error ? `ERROR: ${log.error}\n` : ''}
RAW BODY RECIBIDO:
${rawTruncado}

`;
        });
    }

    return `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Webhook Tracking - Monitor</title>
    <meta http-equiv="refresh" content="10">
</head>
<body>
<pre>
========================================
WEBHOOK TRACKING - MONITOR DE PRUEBAS
========================================
URL de este endpoint: https://${host}
WebSocket URL: wss://${host}/?token=TU_TOKEN

Ultima actualizacion: ${new Date().toISOString()}
Clientes WS conectados: ${clientesConectados.length > 0 ? clientesConectados.join(', ') : 'ninguno'}

(Esta pagina se refresca automaticamente cada 10 segundos)

========================================
NOTIFICACIONES RECIBIDAS (${logs.length})
========================================
${notificacionesHTML}
</pre>
</body>
</html>`;
}

// ================== SERVIDOR HTTP ==================

const server = http.createServer((req, res) => {
    const host = req.headers.host || 'tu-dominio.com';

    if (req.method === 'POST') {
        let body = '';

        req.on('data', (chunk: Buffer) => {
            body += chunk.toString();
        });

        req.on('end', () => {
            const registro = procesarNotificacion(body, req);

            console.log(`[HTTP] Notificacion recibida de ${registro.ip_origen}`);

            if (registro.data_decodificada) {
                console.log('  Tracking:', registro.data_decodificada.tracking_number);
                console.log('  Estado:', registro.data_decodificada.comment);

                // Persistir en PostgreSQL solo si el payload es un objeto JSON real
                // (evitar el fallback { data: "..." } cuando no se pudo parsear)
                if (!esDecodedFallback(registro.data_decodificada) && registro.data_decodificada.tracking_number) {
                    (async () => {
                        try {
                            const trackingCode = await obtenerTrackingCode(registro.data_decodificada!.tracking_number!);
                            await insertarTracking(registro.data_decodificada!, trackingCode);
                        } catch (e) {
                            console.error('Error persistiendo tracking en DB:', (e as Error).message);
                        }
                    })();
                }

                // Retransmitir a clientes WebSocket
                broadcast({
                    tipo: 'tracking',
                    data: registro.data_decodificada
                });

                console.log(`  Enviado a ${clientesWS.size} cliente(s) WebSocket`);
            }

            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ status: 'ok', received: registro.fecha_recepcion }));
        });

    } else if (req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
        res.end(generarHTML(host));

    } else {
        res.writeHead(405);
        res.end('Method not allowed');
    }
});

server.on('upgrade', (req: http.IncomingMessage, socket: net.Socket, head: Buffer) => {
    handleWebSocketUpgrade(req, socket);
});

server.listen(PORT, () => {
    console.log(`Webhook Tracking corriendo en puerto ${PORT}`);
    console.log(`HTTP: http://localhost:${PORT}`);
    console.log(`WebSocket: ws://localhost:${PORT}/?token=***`);
    console.log('');
    console.log('Clientes configurados:', Object.keys(TOKENS_CLIENTES).join(', '));
});
