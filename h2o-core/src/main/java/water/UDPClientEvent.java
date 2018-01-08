package water;

import water.init.NetworkInit;
import water.util.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

/**
 * A simple message which informs cluster about a new client
 * which was connected or about existing client who wants to disconnect.
 * The event is used only in flatfile mode where in case of connecting, it
 * allows the client to connect to a single node, which will
 * inform a cluster about the client. Hence, the rest of nodes will
 * start ping client with heartbeat, and vice versa.
 */
public class UDPClientEvent extends UDP {

  @Override
  AutoBuffer call(AutoBuffer ab) {
    // Handle only by non-client nodes

    // Ignore messages from different cloud
    if(ab._h2o._heartbeat._cloud_name_hash != H2O.SELF._heartbeat._cloud_name_hash) {
      return ab;
    }

    if (ab._h2o != H2O.SELF) {
      ClientEvent ce = new ClientEvent().read(ab);
      switch(ce.type){
        // Connect event is not sent in multicast mode
        case CONNECT:
          if(!H2O.ARGS.client && H2O.isFlatfileEnabled()) {
              Log.info("Client reported via broadcast message " + ce.clientNode + " from " + ab._h2o);
              H2O.addNodeToFlatfile(ce.clientNode);
              H2O.reportClient(ce.clientNode);
          }
          break;
        // Regular disconnect event also doesn't have any effect in multicast mode.
        // However we need to catch the watchdog disconnect event in both multicast and flatfile mode.
        case DISCONNECT:
          if(!H2O.ARGS.client) {
            // handle regular disconnection
            if (H2O.isFlatfileEnabled()) {
              Log.info("Client: " + ce.clientNode + " has been disconnected on: " + ab._h2o);
              H2O.removeNodeFromFlatfile(ce.clientNode);
              H2O.removeClient(ce.clientNode);
            }

            // In case the disconnection comes from the watchdog client, stop the cloud ( in both multicast and flatfile mode )
            if (ce.clientNode._heartbeat._watchdog_client) {
              Log.info("Stopping H2O cloud because watchdog client is disconnecting from the cloud.");
              // client is sending disconnect message on purpose, we can stop the cloud even without asking
              // the rest of the nodes for consensus on this
              H2O.shutdown(0);
            }
          }
          break;
        case CONFIRM_CONNECT:
          if(H2O.ARGS.client && H2O.isFlatfileEnabled()){
            Log.info("Got confirmation from the node who client first contacted");
            for(int i = 0; i<ce.flatfile.length; i++){
              H2O.addNodeToFlatfile(ce.flatfile[i]);
            }
          }
          break;
        default:
          throw new RuntimeException("Unsupported Client event: " + ce.type);
      }
    }

    return ab;
  }

  public static class ClientEvent extends Iced<ClientEvent> {

    public enum Type {
      CONNECT,
      CONFIRM_CONNECT,
      DISCONNECT;

      public void broadcast(H2ONode clientNode) {
        ClientEvent ce = new ClientEvent(this, clientNode);
        ce.write(new AutoBuffer(H2O.SELF, udp.client_event._prior).putUdp(udp.client_event)).close();
      }

      public void confirm(H2ONode clientNode){
        Log.info("Confirming that client " + clientNode + " has been propagated everywhere");

        ClientEvent ce = new ClientEvent(this, clientNode,
                H2O.getFlatfile().toArray(new H2ONode[H2O.getFlatfile().size()]));

        ce.write(new AutoBuffer(clientNode, udp.client_event._prior).putUdp(udp.client_event)).close();
      }
    }

    // Type of client event
    public Type type;
    // Client
    public H2ONode clientNode;
    public H2ONode[] flatfile;

    public ClientEvent() {}
    public ClientEvent(Type type, H2ONode clientNode) {
      this.type = type;
      this.clientNode = clientNode;
    }

    public ClientEvent(Type type, H2ONode clientNode, H2ONode[] flatfile) {
      this.type = type;
      this.clientNode = clientNode;
      this.flatfile = flatfile;
    }
  }
}