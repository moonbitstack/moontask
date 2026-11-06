// The page the interface serves at `/`.
//
// It is one file and no build step: the browser fetches the same JSON the API
// answers, and the live panel follows the event stream. Keeping the page in the
// module rather than in a separate front end is what lets a caller mount the
// whole interface with one call.

///|
/// The dashboard page.
fn page() -> String {
  let head = "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">" +
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">" +
    "<title>moontask</title><style>" +
    style() +
    "</style></head><body>"
  let body = "<header><h1>moontask</h1><p>Queues, tasks and runs of this process.</p></header>" +
    "<main>" +
    "<section><h2>Tasks</h2><table id=\"tasks\"><thead><tr>" +
    "<th>Name</th><th>Queue</th><th>Retries</th><th>Timeout</th><th>Confirmed</th></tr></thead>" +
    "<tbody></tbody></table>" +
    "<form id=\"send\"><label>Task <input name=\"task\" value=\"\"></label>" +
    "<label>Arguments <input name=\"args\" value=\"null\"></label>" +
    "<button type=\"submit\">Send</button></form><p id=\"sent\"></p></section>" +
    "<section><h2>Queues</h2><table id=\"queues\"><thead><tr>" +
    "<th>Name</th><th>Ready</th><th>Delayed</th><th>Unacked</th></tr></thead>" +
    "<tbody></tbody></table></section>" +
    "<section><h2>Live</h2><ol id=\"events\"></ol></section>" +
    "</main><script>" +
    script() +
    "</script></body></html>"
  head + body
}

///|
fn style() -> String {
  ":root{--ink:#1d1d1f;--paper:#f6f4ef;--line:#d9d4c8;--accent:#7a5c3e}" +
  "*{box-sizing:border-box}" +
  "body{margin:0;font:15px/1.5 ui-sans-serif,system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;" +
  "color:var(--ink);background:var(--paper)}" +
  "header{padding:28px 32px 12px;border-bottom:1px solid var(--line)}" +
  "h1{margin:0;font-size:20px;letter-spacing:.02em}" +
  "header p{margin:4px 0 0;color:#6b665c}" +
  "main{display:grid;gap:24px;padding:24px 32px 64px;max-width:900px}" +
  "h2{font-size:14px;text-transform:uppercase;letter-spacing:.08em;color:#6b665c;margin:0 0 8px}" +
  "table{width:100%;border-collapse:collapse}" +
  "th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}" +
  "th{font-weight:600;color:#6b665c}" +
  "form{display:flex;gap:12px;align-items:end;margin-top:12px;flex-wrap:wrap}" +
  "label{display:grid;gap:4px;font-size:13px;color:#6b665c}" +
  "input{font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:6px;background:#fff}" +
  "button{font:inherit;padding:7px 14px;border:1px solid var(--accent);border-radius:6px;" +
  "background:var(--accent);color:#fff;cursor:pointer}" +
  "ol#events{margin:0;padding-left:20px;font-variant-numeric:tabular-nums;color:#4a463e}" +
  "@media (prefers-color-scheme: dark){" +
  ":root{--ink:#eceae4;--paper:#171613;--line:#33302a;--accent:#c49a6c}" +
  "input{background:#211f1b}}"
}

///|
fn script() -> String {
  "const get=async(url)=>(await fetch(url)).json();" +
  "const row=(cells)=>{const tr=document.createElement('tr');" +
  "for(const c of cells){const td=document.createElement('td');td.textContent=c;tr.appendChild(td)}" +
  "return tr};" +
  "const fill=(id,rows)=>{const body=document.querySelector('#'+id+' tbody');" +
  "body.replaceChildren();for(const r of rows){body.appendChild(row(r))}};" +
  "const show=async()=>{const tasks=await get('/api/tasks');" +
  "fill('tasks',tasks.map(t=>[t.name,t.queue,t.retries,t.timeout??'-',t.acks_late?'late':'on receipt']));" +
  "const queues=await get('/api/queues');" +
  "fill('queues',queues.map(q=>[q.name,q.ready,q.delayed,q.unacked]))" +
  "document.querySelector('input[name=task]').value=tasks.length?tasks[0].name:''};" +
  "document.querySelector('#send').addEventListener('submit',async(e)=>{e.preventDefault();" +
  "const form=new FormData(e.target);const name=form.get('task');" +
  "const res=await fetch('/api/tasks/'+encodeURIComponent(name)," +
  "{method:'POST',headers:{'content-type':'application/json'},body:form.get('args')});" +
  "const body=await res.json();" +
  "document.querySelector('#sent').textContent=res.status===202?('sent '+body.id):(body.error||res.status);" +
  "show()});" +
  "const events=new EventSource('/api/events');" +
  "events.onmessage=(m)=>{const list=document.querySelector('#events');" +
  "const item=document.createElement('li');" +
  "item.textContent=new Date().toLocaleTimeString()+' '+m.data;" +
  "list.prepend(item);while(list.children.length>20)list.lastChild.remove()};" +
  "show();setInterval(show,5000)"
}
