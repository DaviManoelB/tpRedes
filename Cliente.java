import javax.swing.*;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Cliente Swing (executar no PC1).
 *
 * - TCP: conecta ao servidor, mantem a conexao aberta e usa uma thread
 *        dedicada para ler as respostas sem travar a interface.
 * - UDP: cada envio roda em uma thread propria, mede o RTT e trata timeout
 *        (UDP nao garante entrega).
 *
 * IP/portas padrao: o IP do roteador R1 (ponto de entrada do redirecionamento)
 * e as portas TCP/UDP configuradas no port forwarding.
 */
public class Cliente extends JFrame {

   // Valores iniciais; podem ser editados na tela
   private static final String IP_PADRAO = "192.168.0.1";
   private static final int PORTA_TCP_PADRAO = 6789;
   private static final int PORTA_UDP_PADRAO = 9871;
   private static final int TIMEOUT_UDP_MS = 3000;

   // Componentes Swing
   private final JTextField campoIp = new JTextField(IP_PADRAO, 12);
   private final JTextField campoPortaTcp = new JTextField(String.valueOf(PORTA_TCP_PADRAO), 5);
   private final JTextField campoPortaUdp = new JTextField(String.valueOf(PORTA_UDP_PADRAO), 5);
   private final JButton botaoConectar = new JButton("Conectar TCP");
   private final JButton botaoDesconectar = new JButton("Desconectar");
   private final JTextField campoMensagem = new JTextField();
   private final JButton botaoEnviarTcp = new JButton("Enviar TCP");
   private final JButton botaoEnviarUdp = new JButton("Enviar UDP");
   private final JTextArea areaLog = new JTextArea();
   private final JLabel status = new JLabel("TCP: desconectado");

   // Estado da conexao TCP
   private Socket socketTcp;
   private OutputStream saidaTcp;
   // Uma unica thread para as escritas TCP garante a ordem das mensagens
   private final ExecutorService escritorTcp = Executors.newSingleThreadExecutor(r -> {
      Thread t = new Thread(r, "Cliente-TCP-escrita");
      t.setDaemon(true);
      return t;
   });

   public Cliente() {
      super("Cliente de Rede - TCP e UDP");
      montarInterface();
      atualizarBotoes(false);
   }

   // ------------------------------------------------------------ interface
   private void montarInterface() {
      setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
      addWindowListener(new WindowAdapter() {
         @Override
         public void windowClosing(WindowEvent e) {
            fecharTcp();
            dispose();
            System.exit(0);
         }
      });

      // Topo: configuracao do servidor
      JPanel topo = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 6));
      topo.add(new JLabel("IP:"));
      topo.add(campoIp);
      topo.add(new JLabel("Porta TCP:"));
      topo.add(campoPortaTcp);
      topo.add(new JLabel("Porta UDP:"));
      topo.add(campoPortaUdp);
      topo.add(botaoConectar);
      topo.add(botaoDesconectar);

      // Centro: log
      areaLog.setEditable(false);
      areaLog.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 13));
      JScrollPane rolagem = new JScrollPane(areaLog);

      // Rodape: mensagem + botoes
      JPanel envio = new JPanel(new BorderLayout(6, 6));
      envio.add(new JLabel("Mensagem:"), BorderLayout.WEST);
      envio.add(campoMensagem, BorderLayout.CENTER);
      JPanel botoes = new JPanel(new FlowLayout(FlowLayout.RIGHT, 6, 0));
      botoes.add(botaoEnviarTcp);
      botoes.add(botaoEnviarUdp);
      envio.add(botoes, BorderLayout.EAST);

      JPanel rodape = new JPanel(new BorderLayout(0, 4));
      rodape.add(envio, BorderLayout.CENTER);
      rodape.add(status, BorderLayout.SOUTH);
      rodape.setBorder(BorderFactory.createEmptyBorder(6, 6, 6, 6));

      add(topo, BorderLayout.NORTH);
      add(rolagem, BorderLayout.CENTER);
      add(rodape, BorderLayout.SOUTH);

      // Eventos
      botaoConectar.addActionListener(e -> conectarTcp());
      botaoDesconectar.addActionListener(e -> desconectarTcp());
      botaoEnviarTcp.addActionListener(e -> enviarTcp());
      botaoEnviarUdp.addActionListener(e -> enviarUdp());
      campoMensagem.addActionListener(e -> {
         // Enter envia por TCP se conectado; senao, por UDP
         if (botaoEnviarTcp.isEnabled()) enviarTcp(); else enviarUdp();
      });

      setSize(720, 480);
      setLocationRelativeTo(null);
   }

   private void atualizarBotoes(boolean conectado) {
      botaoConectar.setEnabled(!conectado);
      botaoDesconectar.setEnabled(conectado);
      botaoEnviarTcp.setEnabled(conectado);
      campoIp.setEnabled(!conectado);
      campoPortaTcp.setEnabled(!conectado);
      status.setText(conectado ? "TCP: conectado a " + campoIp.getText().trim() + ":"
            + campoPortaTcp.getText().trim() : "TCP: desconectado");
   }

   /** Escreve no log a partir de qualquer thread (sempre via Event Dispatch Thread). */
   private void log(String proto, String msg) {
      String hora = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
      SwingUtilities.invokeLater(() -> {
         areaLog.append("[" + hora + "] [" + proto + "] " + msg + "\n");
         areaLog.setCaretPosition(areaLog.getDocument().getLength());
      });
   }

   // ------------------------------------------------------------------ TCP
   private void conectarTcp() {
      final String ip = campoIp.getText().trim();
      final int porta;
      try {
         porta = Integer.parseInt(campoPortaTcp.getText().trim());
      } catch (NumberFormatException ex) {
         log("TCP", "Porta TCP invalida.");
         return;
      }
      botaoConectar.setEnabled(false);
      log("TCP", "Conectando a " + ip + ":" + porta + " ...");

      // Conectar pode demorar: nao pode rodar na thread da interface
      new Thread(() -> {
         try {
            // Primitivas socket + connect (three-way handshake)
            Socket s = new Socket();
            s.connect(new InetSocketAddress(ip, porta), 5000);
            socketTcp = s;
            saidaTcp = s.getOutputStream();
            log("TCP", "Conectado. Local: " + s.getLocalSocketAddress()
                  + " -> Servidor: " + s.getRemoteSocketAddress());
            SwingUtilities.invokeLater(() -> atualizarBotoes(true));

            // Esta thread passa a ser o "ouvinte" das respostas do servidor
            lerRespostasTcp(s);
         } catch (IOException ex) {
            log("TCP", "Falha ao conectar: " + ex.getMessage());
            SwingUtilities.invokeLater(() -> atualizarBotoes(false));
         }
      }, "Cliente-TCP-leitura").start();
   }

   private void lerRespostasTcp(Socket s) {
      try (BufferedReader entrada = new BufferedReader(
            new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
         String linha;
         while ((linha = entrada.readLine()) != null) {
            log("TCP", "FROM SERVER: " + linha);
         }
         log("TCP", "Servidor encerrou a conexao.");
      } catch (IOException ex) {
         if (!s.isClosed()) log("TCP", "Conexao interrompida: " + ex.getMessage());
      } finally {
         fecharTcp();
         SwingUtilities.invokeLater(() -> atualizarBotoes(false));
      }
   }

   private void enviarTcp() {
      final String msg = campoMensagem.getText();
      if (msg.isEmpty() || saidaTcp == null) return;
      campoMensagem.setText("");
      final OutputStream saida = saidaTcp;

      escritorTcp.submit(() -> {
         try {
            // Primitiva send: mensagem terminada em \n (o servidor usa readLine)
            saida.write((msg + "\n").getBytes(StandardCharsets.UTF_8));
            saida.flush();
            log("TCP", "ENVIADO: " + msg);
         } catch (IOException ex) {
            log("TCP", "Erro ao enviar: " + ex.getMessage());
         }
      });
   }

   private void desconectarTcp() {
      escritorTcp.submit(() -> {
         try {
            if (saidaTcp != null) {
               saidaTcp.write("SAIR\n".getBytes(StandardCharsets.UTF_8));
               saidaTcp.flush();
            }
         } catch (IOException ignorado) {
            // a conexao pode ja estar fechada
         }
         fecharTcp();
         log("TCP", "Desconectado.");
      });
   }

   private synchronized void fecharTcp() {
      try {
         if (socketTcp != null && !socketTcp.isClosed()) socketTcp.close(); // primitiva close
      } catch (IOException ignorado) {
         // nada a fazer
      }
      socketTcp = null;
      saidaTcp = null;
   }

   // ------------------------------------------------------------------ UDP
   private void enviarUdp() {
      final String msg = campoMensagem.getText();
      if (msg.isEmpty()) return;
      final String ip = campoIp.getText().trim();
      final int porta;
      try {
         porta = Integer.parseInt(campoPortaUdp.getText().trim());
      } catch (NumberFormatException ex) {
         log("UDP", "Porta UDP invalida.");
         return;
      }
      campoMensagem.setText("");

      // Cada envio UDP roda em sua propria thread: a interface nunca trava
      // esperando a resposta (ou o timeout).
      new Thread(() -> {
         try (DatagramSocket clientSocket = new DatagramSocket()) {
            clientSocket.setSoTimeout(TIMEOUT_UDP_MS);
            InetAddress ipServidor = InetAddress.getByName(ip);

            byte[] sendData = msg.getBytes(StandardCharsets.UTF_8);
            DatagramPacket sendPacket =
                  new DatagramPacket(sendData, sendData.length, ipServidor, porta);

            long inicio = System.nanoTime();
            clientSocket.send(sendPacket);
            log("UDP", "ENVIADO para " + ip + ":" + porta + " (porta local "
                  + clientSocket.getLocalPort() + "): " + msg);

            byte[] receiveData = new byte[1024];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(receivePacket);
            double rttMs = (System.nanoTime() - inicio) / 1_000_000.0;

            String resposta = new String(receivePacket.getData(), 0,
                  receivePacket.getLength(), StandardCharsets.UTF_8);
            log("UDP", String.format("FROM SERVER: %s  (RTT %.2f ms)", resposta, rttMs));
         } catch (SocketTimeoutException ex) {
            log("UDP", "Sem resposta em " + TIMEOUT_UDP_MS
                  + " ms (datagrama perdido ou redirecionamento incorreto).");
         } catch (IOException ex) {
            log("UDP", "Erro: " + ex.getMessage());
         }
      }, "Cliente-UDP").start();
   }

   // ----------------------------------------------------------------- main
   public static void main(String[] args) {
      SwingUtilities.invokeLater(() -> new Cliente().setVisible(true));
   }
}