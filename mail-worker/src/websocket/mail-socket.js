import { DurableObject } from 'cloudflare:workers';

const MAX_DOMAINS_PER_CONNECTION = 100;
const DOMAIN_RE = /^(?=.{1,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,63}$/i;

function normalizeDomain(value) {
	if (typeof value !== 'string') return null;
	const domain = value.trim().toLowerCase().replace(/^\.+|\.+$/g, '');
	if (!DOMAIN_RE.test(domain)) return null;
	return domain;
}

function getRecipientDomain(address) {
	if (typeof address !== 'string') return null;
	// CloudMail stores the SMTP envelope recipient in toEmail. Be defensive
	// if a display-name form is ever supplied by a caller.
	const match = address.match(/<\s*([^<>\s]+@[^<>\s]+)\s*>/);
	const email = (match ? match[1] : address).trim().toLowerCase();
	const at = email.lastIndexOf('@');
	if (at <= 0 || at === email.length - 1) return null;
	return normalizeDomain(email.slice(at + 1));
}

function readSubscriptions(socket) {
	try {
		const attachment = socket.deserializeAttachment();
		return Array.isArray(attachment?.domains) ? attachment.domains : [];
	} catch {
		return [];
	}
}

/**
 * Durable Object WebSocket hub. Clients authenticate at the Worker endpoint,
 * then subscribe to one or more recipient domains using a JSON message:
 * {"action":"subscribe","domains":["example.com"]}
 * Only mail addressed to a subscribed domain is delivered to that socket.
 */
export class MailSocket extends DurableObject {
	async fetch(request) {
		const url = new URL(request.url);

		if (url.pathname === '/ws' && request.method === 'GET') {
			if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
				return new Response('Expected WebSocket', { status: 426 });
			}

			const pair = new WebSocketPair();
			const client = pair[0];
			const server = pair[1];
			this.ctx.acceptWebSocket(server);
			server.serializeAttachment({ domains: [] });
			server.send(JSON.stringify({
				event: 'connected',
				data: {
					message: 'CloudMail WebSocket connected',
					instruction: 'Send {"action":"subscribe","domains":["example.com"]} to subscribe'
				},
				timestamp: new Date().toISOString()
			}));

			return new Response(null, { status: 101, webSocket: client });
		}

		if (url.pathname === '/publish' && request.method === 'POST') {
			const suppliedSecret = request.headers.get('X-CloudMail-Internal') || '';
			// console.info('正在发送消息',suppliedSecret);
			if (!this.env.jwt_secret || suppliedSecret !== this.env.jwt_secret) {
				console.error('正在发送失败',"token无效");
				return new Response('Forbidden', { status: 403 });
			}

			let payload;
			try {
				payload = await request.json();
			} catch {
				console.error('正在发送失败',"参数错误");
				return Response.json({ success: false, message: 'Invalid JSON' }, { status: 400 });
			}

			const domain = getRecipientDomain(payload?.toEmail);
			if (!domain) {
				console.error('正在发送失败',"域名不存在");
				return Response.json({ success: true, domain: null, connections: 0, message: 'Recipient domain unavailable' });
			}

			// console.info('正在发送订阅',domain);
			const message = JSON.stringify({
				event: 'mail.received',
				domain,
				data: payload,
				timestamp: new Date().toISOString()
			});

			let connections = 0;
			for (const socket of this.ctx.getWebSockets()) {
				if (socket.readyState !== WebSocket.OPEN) continue;
				const domains = readSubscriptions(socket);
				if (!domains.includes(domain)){
					console.error('当前域名没有订阅',domain);
					continue;
				}
				try {
					socket.send(message);
					connections++;
					// console.info('消息发送成功');
				} catch (error) {
					console.error('CloudMail WebSocket domain push failed:', error);
				}
			}

			return Response.json({ success: true, domain, connections });
		}

		return new Response('Not Found', { status: 404 });
	}

	async webSocketMessage(socket, message) {
		if (message === 'ping') {
			socket.send('pong');
			return;
		}

		let command;
		try {
			command = JSON.parse(typeof message === 'string' ? message : new TextDecoder().decode(message));
		} catch {
			socket.send(JSON.stringify({ event: 'error', data: { code: 'INVALID_JSON', message: 'Send a JSON command' } }));
			return;
		}

		if (command?.action === 'subscribe') {
			if (!Array.isArray(command.domains) || command.domains.length === 0) {
				socket.send(JSON.stringify({
					event: 'error',
					data: {
						code: 'DOMAINS_REQUIRED',
						message: 'domains must be a non-empty array'
					}
				}));
				return;
			}

			if (command.domains.length > MAX_DOMAINS_PER_CONNECTION) {
				socket.send(JSON.stringify({
					event: 'error',
					data: {
						code: 'TOO_MANY_DOMAINS',
						message: `Maximum ${MAX_DOMAINS_PER_CONNECTION} domains per connection`
					}
				}));
				return;
			}

			const domains = [...new Set(
				command.domains.map(normalizeDomain).filter(Boolean)
			)];

			if (domains.length === 0) {
				socket.send(JSON.stringify({
					event: 'error',
					data: {
						code: 'INVALID_DOMAINS',
						message: 'No valid domains supplied'
					}
				}));
				return;
			}

			// 保存当前 WebSocket 连接的订阅域名
			socket.serializeAttachment({ domains });

			// 打印订阅成功日志
			console.log("SUBSCRIBE_SUCCESS", {
				domains,
				count: domains.length
			});

			// 返回订阅成功消息
			socket.send(JSON.stringify({
				event: 'subscribed',
				data: { domains },
				timestamp: new Date().toISOString()
			}));

			return;
		}
		if (command?.action === 'unsubscribe') {
			const current = readSubscriptions(socket);
			const requested = Array.isArray(command.domains)
				? command.domains.map(normalizeDomain).filter(Boolean)
				: current;
			const domains = current.filter(domain => !requested.includes(domain));
			socket.serializeAttachment({ domains });
			socket.send(JSON.stringify({
				event: 'unsubscribed',
				data: { domains },
				timestamp: new Date().toISOString()
			}));
			return;
		}

		if (command?.action === 'subscriptions') {
			socket.send(JSON.stringify({ event: 'subscriptions', data: { domains: readSubscriptions(socket) } }));
			return;
		}

		socket.send(JSON.stringify({ event: 'error', data: { code: 'UNKNOWN_ACTION', message: 'Supported actions: subscribe, unsubscribe, subscriptions' } }));
	}

	async webSocketClose(socket, code, reason) {
		console.log('External WebSocket client disconnected:', code, reason);
	}

	async webSocketError(socket, error) {
		console.error('External WebSocket client error:', error);
	}
}
