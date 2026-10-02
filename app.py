from flask import Flask,render_template
from flask_socketio import SocketIO,emit
app=Flask(__name__); app.config["SECRET_KEY"]="kolcebros"
socketio=SocketIO(app,cors_allowed_origins="*",async_mode="threading")
S={"count":4,"current":0,"scores":[0]*4,"history":[],"phase":"setup","last":None}
def snap():
 r=[{"i":i,"name":f"УЧАСТНИК {i+1}","score":v} for i,v in enumerate(S["scores"])]
 return {"count":S["count"],"current":S["current"],"name":f"УЧАСТНИК {S['current']+1}","score":S["scores"][S["current"]],"ranking":sorted(r,key=lambda x:(-x["score"],x["i"])),"phase":S["phase"],"last":S["last"]}
def push(): socketio.emit("state",snap())
@app.route("/")
def screen(): return render_template("screen.html")
@app.route("/control")
def control(): return render_template("control.html")
@socketio.on("connect")
def connect(): emit("state",snap())
@socketio.on("setup")
def setup(d):
 n=max(1,min(10,int(d.get("count",4)))); S.update(count=n,current=0,scores=[0]*n,history=[],phase="playing",last=None); push()
@socketio.on("add")
def add(d):
 if S["phase"]!="playing": return
 p=int(d.get("points",0))
 if p not in (10,15,20,25,30): return
 i=S["current"]; S["scores"][i]+=p; S["history"].append((i,p)); S["last"]={"p":p,"nonce":len(S["history"])}; push()
@socketio.on("undo")
def undo():
 if S["phase"]=="playing" and S["history"]:
  i,p=S["history"].pop(); S["scores"][i]=max(0,S["scores"][i]-p); S["last"]=None; push()
@socketio.on("next")
def nxt():
 if S["phase"]!="playing": return
 if S["current"]<S["count"]-1: S["current"]+=1; S["last"]=None
 else: S["phase"]="finished"; S["last"]=None
 push()
@socketio.on("finish")
def finish():
 if S["phase"]=="playing": S["phase"]="finished"; S["last"]=None; push()
@socketio.on("reset")
def reset():
 S.update(current=0,scores=[0]*S["count"],history=[],phase="setup",last=None); push()
if __name__=="__main__": socketio.run(app,host="0.0.0.0",port=5000,allow_unsafe_werkzeug=True)
