import app from './hono/webs';
import { email } from './email/email';
import userService from './service/user-service';
import verifyRecordService from './service/verify-record-service';
import emailService from './service/email-service';
import kvObjService from './service/kv-obj-service';
import oauthService from './service/oauth-service';
import analysisService from './service/analysis-service';
import { MailSocket } from './websocket/mail-socket';

export { MailSocket };
export default {
	 async fetch(req, env, ctx) {

		const url = new URL(req.url)

		 // Public external-project WebSocket endpoint. Every authenticated client
		 // joins one global hub and receives every successfully stored email.
		 if (url.pathname === '/ws') {
			 if (!env.MAIL_SOCKET || !env.jwt_secret) {
				 return new Response('WebSocket service is not configured', { status: 503 });
			 }
			 if (req.method !== 'GET' || req.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
				 return new Response('Expected WebSocket', { status: 426 });
			 }

			 const authorization = req.headers.get('Authorization') || '';
			 const expected = `Bearer ${env.jwt_secret}`;
			 if (authorization !== expected) {
				 return new Response('Unauthorized', { status: 401 });
			 }

			 const id = env.MAIL_SOCKET.idFromName('cloudmail-global-broadcast');
			 const socket = env.MAIL_SOCKET.get(id);
			 // Forward the original upgrade request unchanged so Cloudflare preserves
			 // the WebSocket handshake headers. The DO binding is internal to this Worker.
			 return socket.fetch(req);
		 }

		if (url.pathname.startsWith('/api/')) {
			url.pathname = url.pathname.replace('/api', '')
			req = new Request(url.toString(), req)
			return app.fetch(req, env, ctx);
		}

		 if (['/static/','/attachments/'].some(p => url.pathname.startsWith(p))) {
			 return await kvObjService.toObjResp( { env }, url.pathname.substring(1));
		 }

		return env.assets.fetch(req);
	},
	email: email,
	async scheduled(c, env, ctx) {
		if (c.cron === '*/30 * * * *') {
			await analysisService.refreshEchartsCache({ env })
			return;
		}

		await verifyRecordService.clearRecord({ env })
		await userService.resetDaySendCount({ env })
		await emailService.completeReceiveAll({ env })
		await emailService.autoClean({ env })
		await analysisService.refreshEchartsCache({ env })
		await oauthService.clearNoBindOathUser({ env })
	},
};
