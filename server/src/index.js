const enc = new TextEncoder();
const now = () => Math.floor(Date.now() / 1000);
const json = (x, status = 200, extra = {}) => new Response(JSON.stringify(x), {status, headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store', ...extra}});

async function sha256Bytes(data){const b=await crypto.subtle.digest('SHA-256',data);return new Uint8Array(b)}
async function sha256(s){return [...await sha256Bytes(enc.encode(s))].map(x=>x.toString(16).padStart(2,'0')).join('');}
async function randomToken(){return crypto.randomUUID()+crypto.randomUUID().replaceAll('-','');}
async function pbkdf2(password,salt,iterations=100000){const key=await crypto.subtle.importKey('raw',enc.encode(password),'PBKDF2',false,['deriveBits']);const bits=await crypto.subtle.deriveBits({name:'PBKDF2',salt:enc.encode(salt),iterations,hash:'SHA-256'},key,256);return [...new Uint8Array(bits)].map(x=>x.toString(16).padStart(2,'0')).join('');}
async function passwordRecord(password){const salt=await randomToken();return `pbkdf2$100000$${salt}$${await pbkdf2(password,salt)}`;}
async function verifyPassword(password,record){const p=String(record).split('$');if(p[0]==='pbkdf2'&&p.length===4)return p[3]===await pbkdf2(password,p[2],Number(p[1]));if(p.length===2)return p[1]===await sha256(`${p[0]}:${password}`);return false;}
async function body(req){try{return await req.json()}catch{return null}}
function bearer(req){const h=req.headers.get('authorization')||'';return h.startsWith('Bearer ')?h.slice(7).trim():'';}
async function sessionUser(env,req){const t=bearer(req);if(!t)return null;const h=await sha256(t);return env.DB.prepare('SELECT u.* FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=? AND s.expires_at>?').bind(h,now()).first();}
async function deviceToken(env,req){const t=bearer(req);if(!t)return null;const h=await sha256(t);return env.DB.prepare('SELECT * FROM devices WHERE token_hash=?').bind(h).first();}
async function canAccess(env,user,deviceId,needed='viewer'){if(!user)return false;if(user.role==='admin')return true;const r=await env.DB.prepare('SELECT role FROM device_users WHERE device_id=? AND user_id=?').bind(deviceId,user.id).first();if(!r)return false;const rank={viewer:0,operator:1,admin:2};return (rank[r.role]??-1)>=(rank[needed]??99);}
function validDeviceId(id){return /^[A-Za-z0-9_-]{3,63}$/.test(id)}
function validEmail(e){return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(e)&&e.length<=160}
function cleanSchedules(arr){if(!Array.isArray(arr)||arr.length>64)return null;const clean=[];for(const s of arr){const relay=Number(s.relay),hour=Number(s.hour),minute=Number(s.minute),action=Number(s.action),days=Number(s.days),durationMinutes=Number(s.durationMinutes||0);if(!Number.isInteger(relay)||relay<1||relay>5||!Number.isInteger(hour)||hour<0||hour>23||!Number.isInteger(minute)||minute<0||minute>59||![0,1].includes(action)||!Number.isInteger(days)||days<1||days>127||!Number.isInteger(durationMinutes)||durationMinutes<0||durationMinutes>1439)return null;clean.push({relay,hour,minute,action,days,durationMinutes,enabled:s.enabled?1:0});}return clean;}

async function bootstrap(env){
  const count=await env.DB.prepare('SELECT COUNT(*) c FROM users').first();
  if((count?.c||0)>0)return;
  if(!validEmail(String(env.ADMIN_EMAIL||''))||typeof env.ADMIN_PASSWORD!=='string'||env.ADMIN_PASSWORD.length<12||env.ADMIN_PASSWORD.length>256)throw new Error('Set a valid ADMIN_EMAIL and an ADMIN_PASSWORD secret of 12-256 characters before first request.');
  const rec=await passwordRecord(env.ADMIN_PASSWORD);
  await env.DB.prepare('INSERT INTO users(email,password_hash,role,created_at) VALUES(?,?,?,?)').bind(env.ADMIN_EMAIL.toLowerCase(),rec,'admin',now()).run();
}

async function api(env,req,url){
  await bootstrap(env);const p=url.pathname;
  if(p==='/api/auth/login'&&req.method==='POST'){
    const b=await body(req),email=String(b?.email||'').trim().toLowerCase();if(!validEmail(email)||typeof b?.password!=='string'||b.password.length<1||b.password.length>256)return json({error:'invalid credentials'},401);
    const u=await env.DB.prepare('SELECT * FROM users WHERE email=?').bind(email).first();if(!u||!(await verifyPassword(b.password,u.password_hash)))return json({error:'invalid credentials'},401);
    if(!String(u.password_hash).startsWith('pbkdf2$')){const upgraded=await passwordRecord(b.password);await env.DB.prepare('UPDATE users SET password_hash=? WHERE id=?').bind(upgraded,u.id).run();}
    const t=await randomToken(),ttl=Math.min(Math.max(Number(env.SESSION_TTL_SECONDS||604800),900),2592000);await env.DB.prepare('INSERT INTO sessions(token_hash,user_id,expires_at) VALUES(?,?,?)').bind(await sha256(t),u.id,now()+ttl).run();
    return json({token:t,user:{id:u.id,email:u.email,role:u.role}});
  }
  if(p==='/api/auth/logout'&&req.method==='POST'){const t=bearer(req);if(t)await env.DB.prepare('DELETE FROM sessions WHERE token_hash=?').bind(await sha256(t)).run();return json({ok:true});}
  if(p==='/api/auth/me'&&req.method==='GET'){const u=await sessionUser(env,req);return u?json({user:{id:u.id,email:u.email,role:u.role}}):json({error:'unauthorized'},401);}

  const u=await sessionUser(env,req);
  if(p==='/api/admin/users'&&req.method==='POST'){
    if(!u||u.role!=='admin')return json({error:'forbidden'},403);const b=await body(req),email=String(b?.email||'').trim().toLowerCase(),password=String(b?.password||'');if(!validEmail(email)||password.length<12||password.length>256)return json({error:'valid email and password (12-256 characters) required'},400);const rec=await passwordRecord(password);try{const r=await env.DB.prepare('INSERT INTO users(email,password_hash,role,created_at) VALUES(?,?,?,?)').bind(email,rec,b.role==='viewer'?'viewer':'member',now()).run();return json({ok:true,id:r.meta.last_row_id});}catch{return json({error:'user already exists'},409);}
  }
  if(p==='/api/admin/devices'&&req.method==='POST'){
    if(!u||u.role!=='admin')return json({error:'forbidden'},403);const b=await body(req);const id=String(b?.id||'').trim(),name=String(b?.name||id).trim();if(!validDeviceId(id)||name.length<1||name.length>100)return json({error:'invalid device id/name'},400);const token=await randomToken();try{await env.DB.prepare('INSERT INTO devices(id,name,owner_id,token_hash,created_at) VALUES(?,?,?,?,?)').bind(id,name,u.id,await sha256(token),now()).run();await env.DB.prepare('INSERT OR REPLACE INTO device_users(device_id,user_id,role) VALUES(?,?,?)').bind(id,u.id,'admin').run();return json({ok:true,id,name,token});}catch{return json({error:'device already exists'},409);}
  }
  if(p==='/api/admin/grant'&&req.method==='POST'){
    if(!u||u.role!=='admin')return json({error:'forbidden'},403);const b=await body(req),deviceId=String(b?.deviceId||''),email=String(b?.email||'').toLowerCase();if(!validDeviceId(deviceId)||!validEmail(email))return json({error:'invalid deviceId/email'},400);const x=await env.DB.prepare('SELECT id FROM users WHERE email=?').bind(email).first();if(!x)return json({error:'user not found'},404);await env.DB.prepare('INSERT OR REPLACE INTO device_users(device_id,user_id,role) VALUES(?,?,?)').bind(deviceId,x.id,b.role==='viewer'?'viewer':'operator').run();return json({ok:true});
  }
  if(p==='/api/devices'&&req.method==='GET'){
    if(!u)return json({error:'unauthorized'},401);const cutoff=now()-15;const rows=u.role==='admin'?await env.DB.prepare('SELECT id,name,(last_seen>? AND online=1) online,last_seen,states,enabled FROM devices ORDER BY name').bind(cutoff).all():await env.DB.prepare('SELECT d.id,d.name,(d.last_seen>? AND d.online=1) online,d.last_seen,d.states,d.enabled FROM devices d JOIN device_users du ON du.device_id=d.id WHERE du.user_id=? ORDER BY d.name').bind(cutoff,u.id).all();return json({devices:rows.results||[]});
  }
  const dm=p.match(/^\/api\/devices\/([^/]+)$/);if(dm&&req.method==='GET'){
    if(!u)return json({error:'unauthorized'},401);const id=decodeURIComponent(dm[1]);if(!(await canAccess(env,u,id,'viewer')))return json({error:'forbidden'},403);const d=await env.DB.prepare('SELECT id,name,(last_seen>? AND online=1) online,last_seen,states,enabled FROM devices WHERE id=?').bind(now()-15,id).first();return d?json({device:d}):json({error:'not found'},404);
  }
  const cm=p.match(/^\/api\/devices\/([^/]+)\/relay$/);if(cm&&req.method==='POST'){
    if(!u)return json({error:'unauthorized'},401);const id=decodeURIComponent(cm[1]);if(!(await canAccess(env,u,id,'operator')))return json({error:'forbidden'},403);const b=await body(req),relay=Number(b?.relay),state=Number(b?.state);if(!Number.isInteger(relay)||relay<1||relay>5||![0,1].includes(state))return json({error:'invalid relay/state'},400);await env.DB.prepare('INSERT INTO commands(device_id,relay,state,created_at,expires_at) VALUES(?,?,?,?,?)').bind(id,relay,state,now(),now()+300).run();return json({ok:true});
  }
  const sm=p.match(/^\/api\/devices\/([^/]+)\/schedules$/);if(sm&&req.method==='GET'){
    if(!u)return json({error:'unauthorized'},401);const id=decodeURIComponent(sm[1]);if(!(await canAccess(env,u,id,'viewer')))return json({error:'forbidden'},403);const r=await env.DB.prepare('SELECT id,relay,hour,minute,action,days,enabled,duration_minutes AS durationMinutes FROM schedules WHERE device_id=? ORDER BY id').bind(id).all();return json({schedules:r.results||[]});
  }
  if(sm&&req.method==='POST'){
    if(!u)return json({error:'unauthorized'},401);const id=decodeURIComponent(sm[1]);if(!(await canAccess(env,u,id,'operator')))return json({error:'forbidden'},403);const clean=cleanSchedules((await body(req))?.schedules);if(!clean)return json({error:'invalid schedules'},400);
    const stmts=[env.DB.prepare('DELETE FROM schedules WHERE device_id=?').bind(id),...clean.map(s=>env.DB.prepare('INSERT INTO schedules(device_id,relay,hour,minute,action,days,enabled,duration_minutes) VALUES(?,?,?,?,?,?,?,?)').bind(id,s.relay,s.hour,s.minute,s.action,s.days,s.enabled,s.durationMinutes))];await env.DB.batch(stmts);return json({ok:true,count:clean.length});
  }
  if(p==='/api/device/schedules'&&req.method==='POST'){
    const d=await deviceToken(env,req);if(!d)return json({error:'unauthorized'},401);const b=await body(req);if(b?.deviceId!==d.id)return json({error:'device id mismatch'},403);const clean=cleanSchedules(b.schedules);if(!clean)return json({error:'invalid schedules'},400);const stmts=[env.DB.prepare('DELETE FROM schedules WHERE device_id=?').bind(d.id),...clean.map(s=>env.DB.prepare('INSERT INTO schedules(device_id,relay,hour,minute,action,days,enabled,duration_minutes) VALUES(?,?,?,?,?,?,?,?)').bind(d.id,s.relay,s.hour,s.minute,s.action,s.days,s.enabled,s.durationMinutes))];await env.DB.batch(stmts);return json({ok:true,count:clean.length});
  }
  if(p==='/api/device/poll'&&req.method==='POST'){
    const d=await deviceToken(env,req);if(!d)return json({error:'unauthorized'},401);const b=await body(req);if(b?.deviceId!==d.id)return json({error:'device id mismatch'},403);
    const states=Array.isArray(b.states)?Array.from({length:5},(_,i)=>b.states[i]?1:0):[0,0,0,0,0];const enabled=Array.isArray(b.enabled)?Array.from({length:5},(_,i)=>!!b.enabled[i]):[true,true,true,false,false];
    const ackIds=Array.isArray(b.ackIds)?b.ackIds.filter(x=>Number.isInteger(Number(x))).map(Number).slice(0,64):[];
    await env.DB.prepare('UPDATE devices SET online=1,last_seen=?,states=?,enabled=? WHERE id=?').bind(now(),JSON.stringify(states),JSON.stringify(enabled),d.id).run();
    if(ackIds.length) await env.DB.prepare(`UPDATE commands SET acknowledged_at=? WHERE device_id=? AND id IN (${ackIds.map(()=>'?').join(',')})`).bind(now(),d.id,...ackIds).run();
    await env.DB.prepare('DELETE FROM commands WHERE device_id=? AND ((expires_at>0 AND expires_at<?) OR (acknowledged_at>0 AND acknowledged_at<?))').bind(d.id,now(),now()-30).run();
    const cmds=await env.DB.prepare('SELECT id,relay,state FROM commands WHERE device_id=? AND acknowledged_at=0 AND (expires_at=0 OR expires_at>?) ORDER BY id LIMIT 64').bind(d.id,now()).all();
    const sch=await env.DB.prepare('SELECT id,relay,hour,minute,action,days,enabled,duration_minutes AS durationMinutes FROM schedules WHERE device_id=? ORDER BY id LIMIT 64').bind(d.id).all();
    return json({commands:cmds.results||[],schedules:sch.results||[]});
  }
  return null;
}

export default {async fetch(req,env){const url=new URL(req.url);try{const r=await api(env,req,url);if(r)return r;return env.ASSETS?env.ASSETS.fetch(req):new Response('Not found',{status:404});}catch(e){console.error(e);return json({error:'server error'},500);}}};
