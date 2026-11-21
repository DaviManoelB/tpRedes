import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.text.BadLocationException;
import java.awt.*;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Cliente Swing (executar no PC1; pode abrir varias instancias).
 *
 * - TCP -> CHAT: conexao confiavel; uma thread fica lendo as mensagens dos outros.
 * - UDP -> PING + STATUS ONLINE: a cada 1 s envia um PING; o servidor responde PONG com a
 *          lista de quem esta online. O cliente calcula RTT e % de perda (timeout de 2 s).
 *          Um ping perdido nao e reenviado: o proximo ja traz a informacao atualizada.
 */
public class Cliente extends JFrame {

   private static final String IP_PADRAO = "192.168.0.1";
   private static final int PORTA_TCP_PADRAO = 6789;
   private static final int PORTA_UDP_PADRAO = 9871;
   private static final int INTERVALO_PING_MS = 1000;
   private static final int TIMEOUT_PING_MS = 2000;
   private static final int MAX_LINHAS_LOG = 500;

   // ---- Swing
   private final JTextField campoIp = new JTextField(IP_PADRAO, 11);
   private final JTextField campoPortaTcp = new JTextField(String.valueOf(PORTA_TCP_PADRAO), 5);
   private final JTextField campoPortaUdp = new JTextField(String.valueOf(PORTA_UDP_PADRAO), 5);
   private final JTextField campoNome = new JTextField("Cliente" + (100 + new Random().nextInt(900)), 9);
   private final JButton botaoConectarTcp = new JButton("Conectar chat (TCP)");
   private final JButton botaoDesconectarTcp = new JButton("Desconectar");
   private final JButton botaoIniciarUdp = new JButton("Iniciar ping (UDP)");
   private final JButton botaoPararUdp = new JButton("Parar ping");
   private final JTextField campoMensagem = new JTextField();
   private final JButton botaoEnviarTcp = new JButton("Enviar");
   private final JTextArea areaChat = new JTextArea();
   private final JTextArea areaUdp = new JTextArea();
   private final DefaultListModel<String> modeloOnline = new DefaultListModel<>();
   private final JLabel lblStats = new JLabel();
   private final JLabel status = new JLabel();

   // ---- TCP
   private volatile Socket socketTcp;
   private volatile OutputStream saidaTcp;
   private volatile boolean tcpAtivo = false;

   // ---- UDP
   private volatile DatagramSocket socketUdp;
   private volatile InetAddress ipServidorUdp;
   private volatile int portaServidorUdp;
   private volatile String nomeUdp = "";
   private ScheduledFuture<?> tarefaPing;
   private final AtomicLong seqUdp = new AtomicLong();
   private final ConcurrentHashMap<Long, Long> pendentes = new ConcurrentHashMap<>(); // seq -> nanoTime

   // Estatisticas do ping (acessar sempre com synchronized(this.stats))
   private final Object stats = new Object();
   private int enviados, recebidos, perdidos;
   private double rttUltimo, rttSoma, rttMin, rttMax;

   // Thread unica: escritas TCP (ordem garantida), envio dos pings e timeouts
   private final ScheduledExecutorService agendador = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "Cliente-escrita");
      t.setDaemon(true);
      return t;
   });

   public Cliente() {
      super("Chat TCP + Ping/Status UDP");
      montarInterface();
      atualizarEstado();
      atualizarEstatisticas();
   }

   // ------------------------------------------------------------ interface
   private void montarInterface() {
      setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
      addWindowListener(new WindowAdapter() {
         @Override
         public void windowClosing(WindowEvent e) {
            pararUdp();
            desconectarTcp();
            Timer t = new Timer(400, ev -> System.exit(0)); // tempo para enviar SAIR
            t.setRepeats(false);
            t.start();
         }
      });

      JPanel linha1 = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 4));
      linha1.add(new JLabel("IP:"));
      linha1.add(campoIp);
      linha1.add(new JLabel("Porta TCP:"));
      linha1.add(campoPortaTcp);
      linha1.add(new JLabel("Porta UDP:"));
      linha1.add(campoPortaUdp);
      linha1.add(new JLabel("Nome:"));
      linha1.add(campoNome);

      JPanel linha2 = new JPanel(new FlowLayout(FlowLayout.LEFT, 6, 4));
      linha2.add(botaoConectarTcp);
      linha2.add(botaoDesconectarTcp);
      linha2.add(Box.createHorizontalStrut(20));
      linha2.add(botaoIniciarUdp);
      linha2.add(botaoPararUdp);

      JPanel topo = new JPanel(new GridLayout(2, 1));
      topo.add(linha1);
      topo.add(linha2);

      Font mono = new Font(Font.MONOSPACED, Font.PLAIN, 13);
      areaChat.setEditable(false);
      areaChat.setFont(mono);
      areaUdp.setEditable(false);
      areaUdp.setFont(mono);
      JScrollPane rolagemChat = new JScrollPane(areaChat);
      rolagemChat.setBorder(new TitledBorder("Chat (TCP)"));
      JScrollPane rolagemUdp = new JScrollPane(areaUdp);
      rolagemUdp.setBorder(new TitledBorder("Monitor de ping (UDP)"));
      JSplitPane centro = new JSplitPane(JSplitPane.VERTICAL_SPLIT, rolagemChat, rolagemUdp);
      centro.setResizeWeight(0.6);

      // Painel lateral: quem esta online + estatisticas
      JList<String> listaOnline = new JList<>(modeloOnline);
      JScrollPane rolagemOnline = new JScrollPane(listaOnline);
      rolagemOnline.setBorder(new TitledBorder("Online (UDP)"));
      lblStats.setBorder(new TitledBorder("Estatisticas"));
      JPanel lateral = new JPanel(new BorderLayout());
      lateral.setPreferredSize(new Dimension(200, 0));
      lateral.add(rolagemOnline, BorderLayout.CENTER);
      lateral.add(lblStats, BorderLayout.SOUTH);

      JPanel envio = new JPanel(new BorderLayout(6, 6));
      envio.add(new JLabel("Mensagem:"), BorderLayout.WEST);
      envio.add(campoMensagem, BorderLayout.CENTER);
      envio.add(botaoEnviarTcp, BorderLayout.EAST);
      JPanel rodape = new JPanel(new BorderLayout(0, 4));
      rodape.add(envio, BorderLayout.CENTER);
      rodape.add(status, BorderLayout.SOUTH);
      rodape.setBorder(BorderFactory.createEmptyBorder(6, 6, 6, 6));

      add(topo, BorderLayout.NORTH);
      add(centro, BorderLayout.CENTER);
      add(lateral, BorderLayout.EAST);
      add(rodape, BorderLayout.SOUTH);

      botaoConectarTcp.addActionListener(e -> conectarTcp());
      botaoDesconectarTcp.addActionListener(e -> desconectarTcp());
      botaoIniciarUdp.addActionListener(e -> iniciarUdp());
      botaoPararUdp.addActionListener(e -> pararUdp());
      botaoEnviarTcp.addActionListener(e -> enviarTcp());
      campoMensagem.addActionListener(e -> enviarTcp());

      setSize(900, 560);
      setLocationRelativeTo(null);
   }

   /** Atualiza botoes e status. Pode ser chamado de qualquer thread. */
   private void atualizarEstado() {
      SwingUtilities.invokeLater(() -> {
         boolean udpAtivo = socketUdp != null;
         botaoConectarTcp.setEnabled(!tcpAtivo);
         botaoDesconectarTcp.setEnabled(tcpAtivo);
         botaoEnviarTcp.setEnabled(tcpAtivo);
         botaoIniciarUdp.setEnabled(!udpAtivo);
         botaoPararUdp.setEnabled(udpAtivo);
         campoIp.setEnabled(!tcpAtivo && !udpAtivo);
         campoPortaTcp.setEnabled(!tcpAtivo);
         campoPortaUdp.setEnabled(!udpAtivo);
         campoNome.setEnabled(!tcpAtivo && !udpAtivo);
         status.setText("Chat TCP: " + (tcpAtivo ? "conectado" : "desconectado")
               + "   |   Ping UDP: " + (udpAtivo ? "ativo" : "parado"));
      });
   }

   private void atualizarEstatisticas() {
      final String html;
      synchronized (stats) {
         int total = recebidos + perdidos;
         double perda = total == 0 ? 0 : 100.0 * perdidos / total;
         double media = recebidos == 0 ? 0 : rttSoma / recebidos;
         html = String.format("<html>Enviados: %d<br>Recebidos: %d<br>Perdidos: %d (%.1f%%)<br>"
                     + "RTT ultimo: %.2f ms<br>RTT medio: %.2f ms<br>RTT min/max: %.2f / %.2f ms</html>",
               enviados, recebidos, perdidos, perda, rttUltimo, media,
               recebidos == 0 ? 0 : rttMin, rttMax);
      }
      SwingUtilities.invokeLater(() -> lblStats.setText(html));
   }

   /** Escreve em uma area de texto a partir de qualquer thread. */
   private void escrever(JTextArea area, String msg) {
      String hora = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
      SwingUtilities.invokeLater(() -> {
         area.append("[" + hora + "] " + msg + "\n");
         try { // evita o log crescer sem limite (ping gera 1 linha por segundo)
            if (area.getLineCount() > MAX_LINHAS_LOG) {
               area.replaceRange("", 0, area.getLineEndOffset(99));
            }
         } catch (BadLocationException ignorado) {
            // nada a fazer
         }
         area.setCaretPosition(area.getDocument().getLength());
      });
   }

   private void logChat(String msg) { escrever(areaChat, msg); }
   private void logUdp(String msg) { escrever(areaUdp, msg); }

   private String nomeDigitado() {
      String n = campoNome.getText().replaceAll("[|,\\r\\n]", "_").trim();
      return n.isEmpty() ? "anonimo" : n;
   }

   // ------------------------------------------------------------------ TCP (chat)
   private void conectarTcp() {
      final String ip = campoIp.getText().trim();
      final String nome = nomeDigitado();
      final int porta;
      try {
         porta = Integer.parseInt(campoPortaTcp.getText().trim());
      } catch (NumberFormatException ex) {
         logChat("Porta TCP invalida.");
         return;
      }
      botaoConectarTcp.setEnabled(false);
      logChat("Conectando a " + ip + ":" + porta + " ...");

      new Thread(() -> {
         try {
            Socket s = new Socket();
            s.connect(new InetSocketAddress(ip, porta), 5000); // socket + connect
            socketTcp = s;
            saidaTcp = s.getOutputStream();
            tcpAtivo = true;
            atualizarEstado();
            logChat("Conectado. Local " + s.getLocalSocketAddress()
                  + " -> Servidor " + s.getRemoteSocketAddress());

            // Primeira linha: identifica o cliente para o servidor
            saidaTcp.write(("NOME|" + nome + "\n").getBytes(StandardCharsets.UTF_8));
            saidaTcp.flush();

            lerMensagensTcp(s); // esta thread passa a ser o ouvinte do chat
         } catch (IOException ex) {
            logChat("Falha ao conectar: " + ex.getMessage());
            atualizarEstado();
         }
      }, "Cliente-TCP-leitura").start();
   }

   private void lerMensagensTcp(Socket s) {
      try (BufferedReader entrada = new BufferedReader(
            new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8))) {
         String linha;
         while ((linha = entrada.readLine()) != null) {
            logChat(linha);
         }
         logChat("Servidor encerrou a conexao.");
      } catch (IOException ex) {
         if (!s.isClosed()) logChat("Conexao interrompida: " + ex.getMessage());
      } finally {
         fecharTcp();
      }
   }

   private void enviarTcp() {
      final String msg = campoMensagem.getText();
      final OutputStream saida = saidaTcp;
      if (msg.isEmpty() || saida == null) return;
      campoMensagem.setText("");
      agendador.execute(() -> {
         try {
            saida.write((msg + "\n").getBytes(StandardCharsets.UTF_8)); // send
            saida.flush();
            logChat("Eu: " + msg);
         } catch (IOException ex) {
            logChat("Erro ao enviar: " + ex.getMessage());
         }
      });
   }

   private void desconectarTcp() {
      final OutputStream saida = saidaTcp;
      agendador.execute(() -> {
         try {
            if (saida != null) {
               saida.write("SAIR\n".getBytes(StandardCharsets.UTF_8));
               saida.flush();
            }
         } catch (IOException ignorado) {
            // conexao ja fechada
         }
         fecharTcp();
      });
   }

   private synchronized void fecharTcp() {
      boolean estavaAtivo = tcpAtivo;
      try {
         if (socketTcp != null && !socketTcp.isClosed()) socketTcp.close(); // close
      } catch (IOException ignorado) {
         // nada a fazer
      }
      socketTcp = null;
      saidaTcp = null;
      tcpAtivo = false;
      atualizarEstado();
      if (estavaAtivo) logChat("Desconectado do chat.");
   }

   // ------------------------------------------------------------------ UDP (ping + online)
   private void iniciarUdp() {
      final String ip = campoIp.getText().trim();
      final int porta;
      try {
         porta = Integer.parseInt(campoPortaUdp.getText().trim());
      } catch (NumberFormatException ex) {
         logUdp("Porta UDP invalida.");
         return;
      }
      try {
         ipServidorUdp = InetAddress.getByName(ip);
         portaServidorUdp = porta;
         nomeUdp = nomeDigitado();
         socketUdp = new DatagramSocket(); // socket UDP (sem conexao)
      } catch (IOException ex) {
         logUdp("Erro: " + ex.getMessage());
         return;
      }
      synchronized (stats) { // zera estatisticas
         enviados = recebidos = perdidos = 0;
         rttUltimo = rttSoma = rttMax = 0;
         rttMin = Double.MAX_VALUE;
      }
      atualizarEstatisticas();
      atualizarEstado();

      final DatagramSocket s = socketUdp;
      logUdp("Ping iniciado: " + ip + ":" + porta + " (porta local " + s.getLocalPort() + ")");

      new Thread(() -> receberUdp(s), "Cliente-UDP-leitura").start(); // ouvinte de PONGs
      tarefaPing = agendador.scheduleAtFixedRate(this::enviarPing, 0,
            INTERVALO_PING_MS, TimeUnit.MILLISECONDS);                 // emissor de PINGs
   }

   private void enviarPing() {
      final DatagramSocket s = socketUdp;
      if (s == null) return;
      final long seq = seqUdp.incrementAndGet();
      try {
         byte[] dados = ("PING|" + seq + "|" + nomeUdp).getBytes(StandardCharsets.UTF_8);
         pendentes.put(seq, System.nanoTime());
         s.send(new DatagramPacket(dados, dados.length, ipServidorUdp, portaServidorUdp));
         synchronized (stats) { enviados++; }
         agendador.schedule(() -> verificarTimeout(seq), TIMEOUT_PING_MS, TimeUnit.MILLISECONDS);
      } catch (IOException ex) {
         pendentes.remove(seq);
         logUdp("Erro ao enviar PING: " + ex.getMessage());
      }
   }

   private void verificarTimeout(long seq) {
      if (pendentes.remove(seq) == null) return; // o PONG ja chegou
      synchronized (stats) { perdidos++; }
      logUdp("PING #" + seq + " PERDIDO (sem PONG em " + TIMEOUT_PING_MS + " ms)");
      atualizarEstatisticas();
   }

   private void receberUdp(DatagramSocket s) {
      while (!s.isClosed()) {
         try {
            byte[] buf = new byte[1024];
            DatagramPacket pacote = new DatagramPacket(buf, buf.length);
            s.receive(pacote);
            String msg = new String(pacote.getData(), 0, pacote.getLength(), StandardCharsets.UTF_8);
            String[] p = msg.split("\\|", 3); // PONG | seq | lista

            if (!p[0].equals("PONG") || p.length < 3) continue;
            long seq = Long.parseLong(p[1]);
            Long inicio = pendentes.remove(seq);
            if (inicio == null) continue; // PONG atrasado: o ping ja foi contado como perdido

            double rtt = (System.nanoTime() - inicio) / 1_000_000.0;
            synchronized (stats) {
               recebidos++;
               rttUltimo = rtt;
               rttSoma += rtt;
               rttMin = Math.min(rttMin, rtt);
               rttMax = Math.max(rttMax, rtt);
            }
            logUdp(String.format("PING #%d  RTT %.2f ms", seq, rtt));
            atualizarEstatisticas();
            atualizarOnline(p[2]);
         } catch (IOException | NumberFormatException ex) {
            if (!s.isClosed()) logUdp("Erro ao receber: " + ex.getMessage());
         }
      }
   }

   private void atualizarOnline(String lista) {
      final String[] nomes = lista.isEmpty() ? new String[0] : lista.split(",");
      SwingUtilities.invokeLater(() -> {
         modeloOnline.clear();
         for (String n : nomes) modeloOnline.addElement(n);
      });
   }

   private void pararUdp() {
      final DatagramSocket s = socketUdp;
      if (s == null) return;
      if (tarefaPing != null) tarefaPing.cancel(false);
      try { // avisa o servidor para nos marcar offline imediatamente
         byte[] dados = ("SAIR|0|" + nomeUdp).getBytes(StandardCharsets.UTF_8);
         s.send(new DatagramPacket(dados, dados.length, ipServidorUdp, portaServidorUdp));
      } catch (IOException ignorado) {
         // o servidor nos removera por inatividade
      }
      socketUdp = null;
      pendentes.clear();
      s.close();
      atualizarOnline("");
      atualizarEstado();
      logUdp("Ping parado.");
   }

   // ----------------------------------------------------------------- main
   public static void main(String[] args) {
      SwingUtilities.invokeLater(() -> new Cliente().setVisible(true));
   }
}