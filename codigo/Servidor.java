import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Servidor (executar no PC2).
 *
 * - TCP (porta 6789): CHAT. Precisa de entrega confiavel e em ordem, entao usa TCP.
 *     Uma thread por cliente; cada linha recebida e retransmitida aos demais.
 *     Primeira linha do cliente: "NOME|apelido".
 *
 * - UDP (porta 9871): PING + STATUS ONLINE. Sondagens periodicas e tolerantes a perda,
 *     entao usa UDP (nao ha por que retransmitir um ping antigo).
 *       cliente -> servidor:  PING|seq|nome     SAIR|seq|nome
 *       servidor -> cliente:  PONG|seq|nome1,nome2,...   (quem esta online agora)
 *     Quem fica mais de 5 s sem enviar PING e considerado offline.
 *
 * Uso: java Servidor [portaTCP] [portaUDP]
 */
public class Servidor {

   private static final long LIMITE_OFFLINE_MS = 5000;

   private static int portaTCP = 6789;
   private static int portaUDP = 9871;

   // ---- chat (TCP)
   private static final CopyOnWriteArrayList<ClienteTCP> clientesTCP = new CopyOnWriteArrayList<>();

   // ---- presenca (UDP): endereco do cliente -> quem e e quando foi visto pela ultima vez
   private static class Presenca {
      volatile String nome;
      volatile long ultimoVisto;
      Presenca(String nome) { this.nome = nome; this.ultimoVisto = System.currentTimeMillis(); }
   }
   private static final ConcurrentHashMap<SocketAddress, Presenca> online = new ConcurrentHashMap<>();
   private static volatile DatagramSocket socketUDP;

   public static void main(String[] args) throws Exception {
      if (args.length >= 1) portaTCP = Integer.parseInt(args[0]);
      if (args.length >= 2) portaUDP = Integer.parseInt(args[1]);

      mostrarEnderecosLocais();

      new Thread(Servidor::servidorTCP, "Servidor-TCP").start();
      new Thread(Servidor::servidorUDP, "Servidor-UDP").start();
      Thread vigia = new Thread(Servidor::vigiarPresenca, "Servidor-Vigia");
      vigia.setDaemon(true);
      vigia.start();
   }

   // ------------------------------------------------------------------ TCP
   private static class ClienteTCP {
      final Socket socket;
      final OutputStream saida;
      volatile String nome;

      ClienteTCP(Socket socket) throws IOException {
         this.socket = socket;
         this.saida = socket.getOutputStream();
         this.nome = socket.getRemoteSocketAddress().toString().replace("/", "");
      }

      // synchronized: varias threads podem escrever para o mesmo cliente
      synchronized void enviar(String linha) {
         try {
            saida.write((linha + "\n").getBytes(StandardCharsets.UTF_8));
            saida.flush();
         } catch (IOException e) {
            log("TCP", "Falha ao enviar para " + nome + ": " + e.getMessage());
         }
      }
   }

   private static void difundir(ClienteTCP origem, String linha) {
      for (ClienteTCP c : clientesTCP) {
         if (c != origem) c.enviar(linha);
      }
   }

   private static void servidorTCP() {
      try (ServerSocket socket = new ServerSocket(portaTCP)) { // socket + bind + listen
         log("TCP", "Escutando na porta " + portaTCP);
         while (true) {
            Socket conexao = socket.accept(); // accept
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
      log("TCP", "Conexao aberta: " + cliente.nome);

      try (Socket c = conexao;
           BufferedReader entrada = new BufferedReader(
                 new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8))) {

         String str;
         boolean primeira = true;
         while ((str = entrada.readLine()) != null) {
            if (primeira) {
               primeira = false;
               if (str.startsWith("NOME|")) {
                  cliente.nome = limpar(str.substring(5));
                  log("TCP", cliente.nome + " entrou no chat");
                  difundir(cliente, "*** " + cliente.nome + " entrou no chat");
                  continue;
               }
            }
            if (str.equalsIgnoreCase("SAIR")) break;
            log("TCP", "[" + cliente.nome + "] " + str);
            difundir(cliente, "[" + cliente.nome + "] " + str);
         }
      } catch (IOException e) {
         log("TCP", "Conexao de " + cliente.nome + " encerrada: " + e.getMessage());
      } finally {
         clientesTCP.remove(cliente);
         log("TCP", cliente.nome + " saiu do chat");
         difundir(cliente, "*** " + cliente.nome + " saiu do chat");
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
            new Thread(() -> tratarDatagrama(receivePacket), "UDP-worker").start();
         }
      } catch (IOException e) {
         log("UDP", "Erro no servidor: " + e.getMessage());
      }
   }

   private static void tratarDatagrama(DatagramPacket pacote) {
      SocketAddress origem = pacote.getSocketAddress();
      String msg = new String(pacote.getData(), 0, pacote.getLength(), StandardCharsets.UTF_8);
      String[] p = msg.split("\\|", 3); // comando | seq | nome
      if (p.length < 3) {
         log("UDP", "Datagrama invalido de " + origem + ": " + msg);
         return;
      }
      String comando = p[0];
      String seq = p[1];
      String nome = limpar(p[2]);

      if (comando.equals("SAIR")) {
         if (online.remove(origem) != null) log("UDP", nome + " ficou offline (saiu)");
         return;
      }
      if (!comando.equals("PING")) {
         log("UDP", "Comando desconhecido de " + origem + ": " + msg);
         return;
      }

      // Atualiza a presenca de quem enviou o PING
      Presenca pr = online.get(origem);
      if (pr == null) {
         online.put(origem, new Presenca(nome));
         log("UDP", nome + " ficou online (" + origem.toString().replace("/", "") + ")");
      } else {
         pr.nome = nome;
         pr.ultimoVisto = System.currentTimeMillis();
      }

      // PONG: devolve o mesmo seq (para o cliente medir RTT) + lista de quem esta online
      byte[] pong = ("PONG|" + seq + "|" + String.join(",", listaOnline()))
            .getBytes(StandardCharsets.UTF_8);
      try {
         socketUDP.send(new DatagramPacket(pong, pong.length, origem));
      } catch (IOException e) {
         log("UDP", "Erro ao enviar PONG: " + e.getMessage());
      }
   }

   private static List<String> listaOnline() {
      List<String> nomes = new ArrayList<>();
      for (Presenca p : online.values()) nomes.add(p.nome);
      Collections.sort(nomes);
      return nomes;
   }

   /** Remove quem ficou mais de 5 s sem enviar PING. */
   private static void vigiarPresenca() {
      while (true) {
         try {
            Thread.sleep(1000);
         } catch (InterruptedException e) {
            return;
         }
         long agora = System.currentTimeMillis();
         for (Map.Entry<SocketAddress, Presenca> e : online.entrySet()) {
            if (agora - e.getValue().ultimoVisto > LIMITE_OFFLINE_MS
                  && online.remove(e.getKey(), e.getValue())) {
               log("UDP", e.getValue().nome + " ficou offline (sem PING ha "
                     + (LIMITE_OFFLINE_MS / 1000) + " s)");
            }
         }
      }
   }

   // ------------------------------------------------------------- utilitarios
   /** Remove caracteres que quebrariam o protocolo. */
   private static String limpar(String nome) {
      String n = nome.replaceAll("[|,\\r\\n]", "_").trim();
      if (n.length() > 20) n = n.substring(0, 20);
      return n.isEmpty() ? "anonimo" : n;
   }

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