package rep04;
import java.util.function.Consumer;
public final class HeartbeatMonitor {
 public enum State{DESCONHECIDO,ATIVO,SUSPEITO} private final int partition;private final long timeoutNanos;private final Consumer<String> log;private State state=State.DESCONHECIDO;private long lastNanos=System.nanoTime(),inc=-1,hb;
 public HeartbeatMonitor(int p,long periodMs,int factor,Consumer<String> l){partition=p;timeoutNanos=periodMs*factor*1_000_000L;log=l;}
 public synchronized void onHeartbeat(long i,long h){if(i<inc)return;if(i>inc&&inc>=0)log.accept("REINICIO;p"+partition+";inc="+i);if(i==inc&&h>hb+1&&hb>0)log.accept("HB_PERDIDO;p"+partition+";de="+(hb+1)+";ate="+(h-1));if(state==State.SUSPEITO)log.accept("RETOMA;p"+partition+";inc="+i+";hb="+h);inc=i;hb=h;lastNanos=System.nanoTime();state=State.ATIVO;}
 public synchronized void check(){long e=System.nanoTime()-lastNanos;if(state!=State.SUSPEITO&&e>timeoutNanos){state=State.SUSPEITO;log.accept("SUSPEITA;p"+partition+";silencio_ms="+e/1_000_000);}}
 public synchronized State state(){return state;}
}
