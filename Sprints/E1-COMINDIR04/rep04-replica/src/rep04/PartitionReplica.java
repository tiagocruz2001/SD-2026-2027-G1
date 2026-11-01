package rep04;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

public class PartitionReplica {
    public static void main(String[] args) {
        if(args.length<2){System.out.println("Uso: PartitionReplica <id> <particoes> [portoApi]");return;}
        String id=args[0],api=args.length>2?"http://127.0.0.1:"+args[2]+"/api/v0":"http://127.0.0.1:5001/api/v0";IpfsPubSub pub=new IpfsPubSub(api);List<State> states=new ArrayList<>();
        for(String value:args[1].split(",")){int p=Integer.parseInt(value.trim());try{State s=new State(id,p);states.add(s);new Thread(()->{try{pub.subscribe(Wire.topicData(p),s::apply);}catch(Exception e){System.out.println("Subscricao p"+p+" interrompida: "+e.getMessage());}},"replica-p"+p).start();}catch(IOException e){System.out.println("IO: "+e.getMessage());}}
        ScheduledExecutorService checks=Executors.newSingleThreadScheduledExecutor();checks.scheduleAtFixedRate(()->states.forEach(s->s.monitor.check()),Wire.VERIFICACAO_MS,Wire.VERIFICACAO_MS,TimeUnit.MILLISECONDS);
    }
    private static final class State{
        final int partition; final RecordFile file;final Map<Long,SensorRecord> pending=new TreeMap<>();final HeartbeatMonitor monitor;final PrintWriter log;long last;String host;int port;
        State(String id,int p)throws IOException{partition=p;file=new RecordFile("replica-"+id+"-p"+p+".txt");last=file.lastSeq();log=new PrintWriter(new FileWriter("monitor-replica"+id+".txt",true),true);monitor=new HeartbeatMonitor(p,Wire.PERIODO_HB_MS,Wire.FATOR_TIMEOUT,e->event(e));}
        void event(String e){String line=System.currentTimeMillis()+";"+e;System.out.println(line);log.println(line);}
        synchronized void apply(String message){try{if(!"LOTE".equals(Wire.type(message)))throw new IllegalArgumentException();String body=Wire.body(message);String[] h=body.split("\\R",2);int n=Integer.parseInt(h[0].split(";",-1)[6]);Batch b=Batch.decode(message,n);host=b.address;port=b.port;monitor.onHeartbeat(b.inc,b.hb);String range=b.records.isEmpty()?"-":b.records.get(0).getSeq()+".."+b.records.get(b.records.size()-1).getSeq();event("RECEBIDO;p"+partition+";inc="+b.inc+";hb="+b.hb+";ultimoSeq="+b.lastSeq+";n="+n+";seq="+range);for(SensorRecord r:b.records){if(r.getSeq()==last+1){append(r);drain();}else if(r.getSeq()>last+1)pending.put(r.getSeq(),r);}if(last<b.lastSeq)recover(last+1,b.lastSeq);}catch(Exception e){System.out.println("REJEITADO: "+message);}}
        void append(SensorRecord r)throws IOException{file.append(r);last=r.getSeq();}void drain()throws IOException{while(pending.containsKey(last+1))append(pending.remove(last+1));}
        void recover(long from,long to){try(Socket s=new Socket()){s.connect(new InetSocketAddress(host,port),Wire.T_TCP_MS);s.setSoTimeout(Wire.T_TCP_MS);PrintWriter out=new PrintWriter(new OutputStreamWriter(s.getOutputStream(),StandardCharsets.UTF_8),true);BufferedReader in=new BufferedReader(new InputStreamReader(s.getInputStream(),StandardCharsets.UTF_8));out.println("PEDIDO "+from+" "+to);String line;while((line=in.readLine())!=null&&!line.equals("FIM")){SensorRecord r=SensorRecord.fromLine(line);if(r.getSeq()==last+1){append(r);drain();}else pending.put(r.getSeq(),r);}event("RECUPERACAO;p"+partition+";de="+from+";ate="+to);}catch(Exception e){System.out.println("RECUPERACAO FALHOU: "+e.getMessage());}}
    }
}
