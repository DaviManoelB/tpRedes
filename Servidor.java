import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Servidor de chat (executar no PC2).
 *
 * Toda mensagem recebida de um cliente e retransmitida aos DEMAIS clientes
 * conectados, seja por TCP ou por UDP (um cliente TCP tambem fala com um UDP).
 *
 * - TCP: conexao persistente; uma thread por cliente. Mensagem = uma linha de texto.
 * - UDP: sem conexao. O cliente se registra com "ENTRAR|seq"; o servidor guarda o
 *        endereco dele e retransmite. Protocolo (texto, campos separados por '|'):
 *           cliente -> servidor:  ENTRAR|seq   MSG|seq|texto   SAIR|seq
 *           servidor -> cliente:  ACK|seq      MSG|origem|texto
 *
 * Uso: java Servidor [portaTCP] [portaUDP]   (padrao: 6789 e 9871)
 */
public class Servidor {

   private static int portaTCP = 6789;
   private static int portaUDP = 9871;

   // Clientes conectados (estruturas seguras para uso por varias threads)
   private static final CopyOnWriteArrayList<ClienteTCP> clientesTCP = new CopyOnWriteArrayList<>();
   private static final Set<SocketAddress> clientesUDP = ConcurrentHashMap.newKeySet();
   private static volatile DatagramSocket socketUDP;

   public static void main(String[] args) throws Exception {
      if (args.length >= 1) portaTCP = Integer.parseInt(args[0]);
      if (args.length >= 2) portaUDP = Integer.parseInt(args[1]);

      mostrarEnderecosLocais();

      new Thread(Servidor::servidorTCP, "Servidor-TCP").start();
      new Thread(Servidor::servidorUDP, "Servidor-UDP").start();
   }

   // ------------------------------------------------------- retransmissao
   /** Envia a mensagem a todos os clientes (TCP e UDP), exceto a origem. */
   private static void difundir(Object origem, String rotulo, String texto) {
      for (ClienteTCP c : clientesTCP) {
         if (c != origem) c.enviar("[" + rotulo + "] " + texto);
      }
      byte[] dados = ("MSG|" + rotulo + "|" + texto).getBytes(StandardCharsets.UTF_8);
      for (SocketAddress destino : clientesUDP) {
         if (destino.equals(origem)) continue;
         try {
            socketUDP.send(new DatagramPacket(dados, dados.length, destino));
         } catch (IOException e) {
            log("UDP", "Falha ao enviar para " + destino + ": " + e.getMessage());
         }
      }
   }

   // ------------------------------------------------------------------ TCP
   private static class ClienteTCP {
      final Socket socket;
      final OutputStream saida;
      final String rotulo;

      ClienteTCP(Socket socket) throws IOException {
         this.socket = socket;
         this.saida = socket.getOutputStream();
         this.rotulo = "TCP " + socket.getRemoteSocketAddress().toString().replace("/", "");
      }

      // synchronized: varias threads podem retransmitir para este cliente ao mesmo tempo
      synchronized void enviar(String linha) {
         try {
            saida.write((linha + "\n").getBytes(StandardCharsets.UTF_8));
            saida.flush();
         } catch (IOException e) {
            log("TCP", "Falha ao enviar para " + rotulo + ": " + e.getMessage());
         }
      }
   }

   private static void servidorTCP() {
      // Primitivas socket + bind + listen
      try (ServerSocket socket = new ServerSocket(portaTCP)) {
         log("TCP", "Escutando na porta " + portaTCP);
         while (true) {
            Socket conexao = socket.accept(); // primitiva accept
            // Uma thread por cliente permite atender varios ao mesmo tempo
            new Thread(() -> atenderClienteTCP(conexao), "TCP-cliente").start();
         }
      } catch (IOException e) {
         log("TCP", "Erro no servidor: " + e.getMessage());
      }
   }

   private static void atenderClienteTCP(Socket conexao) {
      ClienteTCP cliente;
      try {
         cliente = new ClienteTCP(conexao);
      } catch (IOException e) {
         return;
      }
      clientesTCP.add(cliente);
      log("TCP", "Cliente conectado: " + cliente.rotulo
            + " (online: " + clientesTCP.size() + " TCP, " + clientesUDP.size() + " UDP)");

      try (Socket c = conexao;
           BufferedReader entrada = new BufferedReader(
                 new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8))) {
         String str;
         while ((str = entrada.readLine()) != null) {
            if (str.equalsIgnoreCase("SAIR")) break;
            log("TCP", "Recebido de " + cliente.rotulo + ": " + str);
            difundir(cliente, cliente.rotulo, str);
         }
      } catch (IOException e) {
         log("TCP", "Conexao com " + cliente.rotulo + " encerrada: " + e.getMessage());
      } finally {
         clientesTCP.remove(cliente);
         log("TCP", "Cliente desconectado: " + cliente.rotulo);
      }
   }

   // ------------------------------------------------------------------ UDP
   private static void servidorUDP() {
      try (DatagramSocket serverSocket = new DatagramSocket(portaUDP)) {
         socketUDP = serverSocket;
         log("UDP", "Escutando na porta " + portaUDP);
         while (true) {
            byte[] receiveData = new byte[1024]; // buffer novo por datagrama
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            serverSocket.receive(receivePacket);
            // Cada datagrama e tratado em uma thread propria
            new Thread(() -> tratarDatagrama(receivePacket), "UDP-worker").start();
         }
      } catch (IOException e) {
         log("UDP", "Erro no servidor: " + e.getMessage());
      }
   }

   private static void tratarDatagrama(DatagramPacket pacote) {
      SocketAddress origem = pacote.getSocketAddress();
      String rotulo = "UDP " + origem.toString().replace("/", "");
      String msg = new String(pacote.getData(), 0, pacote.getLength(), StandardCharsets.UTF_8);
      String[] p = msg.split("\\|", 3); // comando | seq | texto

      if (p.length < 2) {
         log("UDP", "Datagrama invalido de " + rotulo + ": " + msg);
         return;
      }
      String comando = p[0];
      String seq = p[1];

      switch (comando) {
         case "ENTRAR":
            clientesUDP.add(origem);
            log("UDP", "Cliente registrado: " + rotulo
                  + " (online: " + clientesTCP.size() + " TCP, " + clientesUDP.size() + " UDP)");
            break;
         case "SAIR":
            clientesUDP.remove(origem);
            log("UDP", "Cliente saiu: " + rotulo);
            return; // nao precisa de ACK
         case "MSG":
            if (p.length < 3) return;
            clientesUDP.add(origem); // garante registro mesmo se o ENTRAR se perdeu
            log("UDP", "Recebido de " + rotulo + ": " + p[2]);
            difundir(origem, rotulo, p[2]);
            break;
         default:
            log("UDP", "Comando desconhecido de " + rotulo + ": " + msg);
            return;
      }

      // Confirmacao ao remetente (o cliente usa para medir RTT e detectar perda)
      byte[] ack = ("ACK|" + seq).getBytes(StandardCharsets.UTF_8);
      try {
         socketUDP.send(new DatagramPacket(ack, ack.length, origem));
      } catch (IOException e) {
         log("UDP", "Erro ao enviar ACK: " + e.getMessage());
      }
   }

   // ------------------------------------------------------------- utilitarios
   /** Lista os IPs da maquina, para confirmar o par (IPSERV, PORT). */
   private static void mostrarEnderecosLocais() throws SocketException {
      System.out.println("=== Servidor iniciado ===");
      for (NetworkInterface ni : Collections.list(NetworkInterface.getNetworkInterfaces())) {
         if (!ni.isUp() || ni.isLoopback()) continue;
         for (InetAddress ia : Collections.list(ni.getInetAddresses())) {
            if (ia instanceof Inet4Address) {
               System.out.println("IPSERV candidato: " + ia.getHostAddress()
                     + "  (" + ni.getDisplayName() + ")");
            }
         }
      }
      System.out.println("PORT TCP: " + portaTCP + " | PORT UDP: " + portaUDP);
      System.out.println("=========================");
   }

   private static synchronized void log(String proto, String msg) {
      String hora = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
      System.out.println("[" + hora + "] [" + proto + "] " + msg);
   }
}