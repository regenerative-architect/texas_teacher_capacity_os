import {put,all} from "./storage.js";

const APP_ID="org.planetaryrestorationarchive.texas-learning-capacity.v3";
const TRYSTERO_URL="https://esm.run/trystero";

function uuid(){return crypto.randomUUID?.()||`${Date.now()}-${Math.random().toString(16).slice(2)}`}
function stamp(x){return Date.parse(x?.updated||x?.created||x?.ts||0)||0}
function mergeLww(local=[],incoming=[]){
  const map=new Map(local.map(x=>[x.id,x]));
  for(const x of incoming||[]){
    if(!x?.id)continue;
    const prev=map.get(x.id);
    if(!prev||stamp(x)>=stamp(prev))map.set(x.id,x);
  }
  return [...map.values()];
}
function mergeLog(local=[],incoming=[]){
  const map=new Map(local.map(x=>[x.id,x]));
  for(const x of incoming||[])if(x?.id&&!map.has(x.id))map.set(x.id,x);
  return [...map.values()].sort((a,b)=>stamp(a)-stamp(b)).slice(-500);
}

export class CollaborationClient extends EventTarget{
  constructor(){
    super();
    this.roomId=null;this.person=null;this.networkRoom=null;this.peerId=null;
    this.peerConnected=false;this.libraryReady=false;this.participants=[];this.items=[];this.log=[];
    this.actions={};this.channel=null;
  }
  get room(){return this.roomId}
  get visibleItems(){return this.items.filter(x=>!x.deleted)}
  async join({room,name,role,domain,supervised}){
    if(!room||!name)throw new Error("Room and display name are required.");
    if(!supervised)throw new Error("Confirm adult/approved-organization participation before joining.");
    this.leave();
    this.roomId=String(room).trim().toUpperCase().replace(/[^A-Z0-9_-]/g,"").slice(0,40);
    this.person={id:uuid(),name:String(name).trim().slice(0,60),role:String(role).slice(0,80),domain:String(domain).slice(0,80)};
    await this.#loadLocal();
    this.#startBroadcastChannel();
    this.#broadcastLocal({type:"presence",person:this.person,reply:false});
    this.#broadcastLocal({type:"state",items:this.items,log:this.log});

    let mod;
    try{mod=await import(TRYSTERO_URL);this.libraryReady=true}catch(e){
      this.libraryReady=false;this.#emit("connection");this.#emit("state");
      throw new Error("P2P library could not load. Local same-browser collaboration is still available; reconnect to the internet and retry for cross-device rooms.");
    }
    const {joinRoom,selfId}=mod;this.peerId=selfId;
    const password=`tx-learning:${this.roomId}`;
    this.networkRoom=joinRoom({appId:APP_ID,password},this.roomId);
    this.#wireActions();
    this.networkRoom.onPeerJoin=peerId=>{
      this.peerConnected=true;
      this.actions.presence.send(this.person,{target:peerId});
      this.actions.state.send({items:this.items,log:this.log,updated:new Date().toISOString()},{target:peerId});
      this.#emit("connection");
    };
    this.networkRoom.onPeerLeave=peerId=>{
      this.participants=this.participants.filter(x=>x.peerId!==peerId);
      this.peerConnected=Object.keys(this.networkRoom?.getPeers?.()||{}).length>0;
      this.#emit("state");this.#emit("connection");
    };
    const existing=Object.keys(this.networkRoom?.getPeers?.()||{});
    if(existing.length){
      this.peerConnected=true;
      for(const peerId of existing){
        this.actions.presence.send(this.person,{target:peerId});
        this.actions.state.send({items:this.items,log:this.log,updated:new Date().toISOString()},{target:peerId});
      }
    }
    this.#emit("connection");this.#emit("state");
  }
  async #loadLocal(){
    const rooms=await all("roomcache");const c=rooms.find(x=>x.id===`room:${this.roomId}`);
    this.items=c?.items||[];this.log=c?.log||[];
  }
  async #persist(){
    if(!this.roomId)return;
    await put("roomcache",{id:`room:${this.roomId}`,room:this.roomId,items:this.items,log:this.log.slice(-500),updated:new Date().toISOString()});
  }
  #startBroadcastChannel(){
    if(!("BroadcastChannel" in window))return;
    this.channel?.close();this.channel=new BroadcastChannel(`txlo:${this.roomId}`);
    this.channel.onmessage=e=>{const m=e.data;if(!m||m.sender===this.person?.id)return;this.#applyLocalMessage(m)};
  }
  #wireActions(){
    this.actions.presence=this.networkRoom.makeAction("presence-v3");
    this.actions.state=this.networkRoom.makeAction("state-v3");
    this.actions.item=this.networkRoom.makeAction("item-v3");
    this.actions.feed=this.networkRoom.makeAction("feed-v3");
    this.actions.presence.onMessage=(person,{peerId})=>{
      if(!person?.id)return;
      this.participants=this.participants.filter(x=>x.peerId!==peerId&&x.id!==person.id);
      this.participants.push({...person,peerId});this.#emit("state");
    };
    this.actions.state.onMessage=(data)=>{
      this.items=mergeLww(this.items,data?.items||[]);this.log=mergeLog(this.log,data?.log||[]);
      this.#persist();this.#emit("state");
    };
    this.actions.item.onMessage=(item)=>{this.#acceptItem(item,false)};
    this.actions.feed.onMessage=(entry)=>{this.#acceptFeed(entry,false)};
  }
  #broadcastLocal(msg){
    this.channel?.postMessage({...msg,sender:this.person?.id,room:this.roomId});
  }
  #applyLocalMessage(m){
    if(m.room!==this.roomId)return;
    if(m.type==="item")this.#acceptItem(m.item,false,false);
    if(m.type==="feed")this.#acceptFeed(m.entry,false,false);
    if(m.type==="presence"&&m.person){
      this.participants=this.participants.filter(x=>x.id!==m.person.id);this.participants.push({...m.person,peerId:"local-tab"});this.#emit("state");
      if(!m.reply)this.#broadcastLocal({type:"presence",person:this.person,reply:true});
    }
    if(m.type==="state"){
      this.items=mergeLww(this.items,m.items||[]);this.log=mergeLog(this.log,m.log||[]);this.#persist();this.#emit("state");
    }
  }
  #acceptItem(item,rebroadcast=true,localBroadcast=true){
    if(!item?.id)return;
    this.items=mergeLww(this.items,[item]);this.#persist();this.#emit("state");
    if(rebroadcast&&this.actions.item)this.actions.item.send(item).catch(()=>{});
    if(localBroadcast)this.#broadcastLocal({type:"item",item});
  }
  #acceptFeed(entry,rebroadcast=true,localBroadcast=true){
    if(!entry?.id)return;
    this.log=mergeLog(this.log,[entry]);this.#persist();this.#emit("state");
    if(rebroadcast&&this.actions.feed)this.actions.feed.send(entry).catch(()=>{});
    if(localBroadcast)this.#broadcastLocal({type:"feed",entry});
  }
  createItem(data){
    if(!this.roomId)throw new Error("Join a room first.");
    const now=new Date().toISOString();const item={id:uuid(),room:this.roomId,created:now,updated:now,createdBy:this.person,...data};
    this.#acceptItem(item,true,true);return item;
  }
  updateItem(id,patch){
    const old=this.items.find(x=>x.id===id);if(!old)return;
    const item={...old,...patch,updated:new Date().toISOString(),updatedBy:this.person};this.#acceptItem(item,true,true);
  }
  deleteItem(id){this.updateItem(id,{deleted:true})}
  post(text,kind="note",domain="Education"){
    if(!this.roomId)throw new Error("Join a room first.");
    const entry={id:uuid(),text:String(text).slice(0,4000),kind,domain,person:this.person,ts:new Date().toISOString()};
    this.#acceptFeed(entry,true,true);return entry;
  }
  shareState(){
    const state={type:"state",items:this.items,log:this.log};this.#broadcastLocal(state);
    this.actions.state?.send({items:this.items,log:this.log,updated:new Date().toISOString()}).catch(()=>{});
  }
  #emit(type){this.dispatchEvent(new CustomEvent(type,{detail:this}))}
  leave(){
    try{this.networkRoom?.leave()}catch{}
    try{this.channel?.close()}catch{}
    this.networkRoom=null;this.channel=null;this.peerConnected=false;this.libraryReady=false;this.participants=[];this.actions={};this.roomId=null;this.person=null;this.#emit("connection");
  }
}
