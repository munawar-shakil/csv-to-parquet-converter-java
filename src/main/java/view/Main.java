package view;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import utils.ConvertUtils;
import utils.Log;

import javax.swing.*;
import javax.swing.filechooser.FileFilter;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.filechooser.FileSystemView;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.IOException;

@SpringBootApplication
public class Main implements CommandLineRunner {
    final Log log = Log.getLog(Main.class);

    public static void main(String[] args) {
        new SpringApplicationBuilder(Main.class).headless(false).run(args);
    }

    private String csvFileName = "";
    public static JLabel message = new JLabel("Press Open");

    @Override
    public void run(String... args) {
        final JFrame frame = new JFrame("CSV to Parquet Converter");
        final JCheckBox enableDataCompression = new JCheckBox("Enable data compression");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setSize(600, 300);
        JPanel panel = new JPanel(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
//        c.fill = GridBagConstraints.HORIZONTAL;
        JButton button1 = new JButton("Convert");

        // button to open open dialog
        JButton button2 = new JButton("Open");
        // set the label to its initial value
        JLabel l = new JLabel("No file selected");

//        button1.addActionListener(f1);
        final JLabel finalL = l;

        button1.addActionListener(new ActionListener() {

            @Override
            public void actionPerformed(ActionEvent e) {
                if (csvFileName != null && !csvFileName.isEmpty()) {
                    JFileChooser fileChooser = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
                    fileChooser.setDialogTitle("Specify a file to save");
                    fileChooser.setFileFilter(new FileNameExtensionFilter("Parquet", "parquet"));
                    fileChooser.setSelectedFile(new File(csvFileName.substring(0, csvFileName.length() - ".csv".length()) + ".parquet"));
                    int userSelection = fileChooser.showSaveDialog(frame);

                    if (userSelection == JFileChooser.APPROVE_OPTION) {
                        File fileToSave = fileChooser.getSelectedFile();
                        if (!fileToSave.getAbsolutePath().endsWith(".parquet")) {
                            fileToSave = new File(fileToSave.getParentFile(), fileToSave.getName() + ".parquet");
                        }
                        System.out.println("Save as file: " + fileToSave.getAbsolutePath());
                        try {
                            int extraColumn = convertToParquetAndSave(fileToSave, 0, enableDataCompression.isSelected());
                            if(extraColumn > 0) {
                                int result = JOptionPane.showConfirmDialog(frame,
                                        "<html>At least one row in the csv file has more columns than header.<br/> " +
                                                "The generated file does not have those columns. Do you still want them? <br/>" +
                                                "Clicking yes will regenerate the parquet from beginning with all data.</html>",
                                        "Data without header",
                                        JOptionPane.YES_NO_OPTION,
                                        JOptionPane.QUESTION_MESSAGE);
                                if(result == JOptionPane.YES_OPTION){
                                    boolean old = ConvertUtils.allowOverwrite;
                                    ConvertUtils.setAllowOverwrite(true);
                                    convertToParquetAndSave(fileToSave, extraColumn, enableDataCompression.isSelected());
                                    ConvertUtils.setAllowOverwrite(old);
                                }
                            }
                            message.setText("The csv file has been converted successfully and saved as " + fileToSave.getAbsolutePath());
                        } catch (Exception ex) {
                            String errorMessage = "Unable to convert.<br/>" + ex.getMessage() + "<br/> Please run command 'java -jar " + csvFileName + "',<br/> process the same and report with all the content from console.";
                            if (ConvertUtils.lastProcessedLineNumber != -1) {
                                errorMessage = "Error at row " + (ConvertUtils.lastProcessedLineNumber + 1) + ".<br/>" + errorMessage;
                            }
                            message.setText("<html>" + errorMessage + "</html>");
                        }
                    }
                }
            }
        });
        button2.addActionListener(
                new ActionListener() {
                    @Override
                    public void actionPerformed(ActionEvent e) {
                        message.setText("");
                        JFileChooser j = new JFileChooser(FileSystemView.getFileSystemView().getHomeDirectory());
                        j.setFileFilter(new FileFilter() {
                            @Override
                            public boolean accept(File file) {
                                if (file.isDirectory()) {
                                    return true;
                                } else {
                                    String filename = file.getName().toLowerCase();
                                    return filename.endsWith(".csv");
                                }
                            }

                            @Override
                            public String getDescription() {
                                return null;
                            }
                        });
                        int r = j.showOpenDialog(null);
                        if (r == JFileChooser.APPROVE_OPTION) {
                            // set the label to the path of the selected file
                            csvFileName = j.getSelectedFile().getAbsolutePath();
                            finalL.setText(csvFileName);
                            message.setText("Press Convert.");
                        } else {
                        }


                    }
                });
        c.weightx = 1;
        c.gridx = 0;
        c.gridy = 0;
        panel.add(button1, c);
        c.gridx = 1;
        panel.add(button2, c);
        c.gridx = 2;
        c.weightx = 2;
        c.gridwidth = 2;
        panel.add(l, c);

        c.gridy = 1;
        ButtonGroup bg = new ButtonGroup();
        JRadioButton commaSeparated = new JRadioButton("Comma( , )");
        JRadioButton barSeparated = new JRadioButton("Bar( | )");
        JRadioButton tabSeparated = new JRadioButton("Tab");
        bg.add(commaSeparated);
        bg.add(barSeparated);
        bg.add(tabSeparated);
        c.ipady = 40;
        c.gridx = 0;
        c.gridwidth = 1;
        c.weightx = 1;
        panel.add(new JLabel("Separator: "), c);
        c.gridx = 1;
        panel.add(commaSeparated, c);
        c.gridx = 2;
        panel.add(barSeparated, c);
        c.gridx = 3;
        panel.add(tabSeparated, c);
        commaSeparated.setSelected(true);
        ConvertUtils.setCsvDelimiter(",");
        commaSeparated.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ConvertUtils.setCsvDelimiter(",");
            }
        });
        barSeparated.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ConvertUtils.setCsvDelimiter("|");
            }
        });
        tabSeparated.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                ConvertUtils.setCsvDelimiter("\t");
            }
        });

        c.gridy = 3;
        c.gridx = 0;
        c.weightx = 0;
        c.gridwidth = 1;
        c.ipady = 0;

        final JCheckBox allowOverwrite = new JCheckBox("Overwrite file");
        allowOverwrite.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent actionEvent) {
                ConvertUtils.setAllowOverwrite(allowOverwrite.isSelected());
            }
        });
        panel.add(allowOverwrite, c);
        enableDataCompression.setSelected(true);
        c.gridx = 2;
        panel.add(enableDataCompression, c);
        c.gridx = 0;
        c.gridy = 4;
        c.gridwidth = 4;
        c.ipady = 10;
        panel.add(message, c);
        frame.setContentPane(panel);
        frame.setVisible(true);
    }

    private int convertToParquetAndSave(File output, int extraColumns, boolean enableDataCompression) throws IOException {
        return ConvertUtils.convertCsvToParquet(new File(csvFileName), output,enableDataCompression, true, extraColumns);
    }
}