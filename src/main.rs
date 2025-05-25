use eframe::egui;
use eframe::egui::{ScrollArea, TextWrapMode};
use egui::widgets::Label;
use egui_extras::{Column, TableBuilder};
use polars::prelude::*;
use rfd::FileDialog;
use std::path::PathBuf;

struct Tablr {
    dataframe: Option<DataFrame>,
    column_names: Vec<String>,
    files_to_load: Vec<PathBuf>,
    error_message: Option<String>,
    files_message: String,
}

impl Tablr {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        Self {
            dataframe: None,
            column_names: Vec::new(),
            files_to_load: Vec::new(),
            error_message: None,
            files_message: String::new(),
        }
    }

    fn load_parquet_data(&mut self, paths: &Vec<PathBuf>) {
        self.dataframe = None;
        self.column_names.clear();
        self.error_message = None;

        let scan_sources =
            ScanSources::Paths(paths.iter().map(|p| p.to_path_buf()).collect::<Arc<_>>());

        match LazyFrame::scan_parquet_sources(scan_sources, ScanArgsParquet::default()) {
            Ok(lazy_frame) => match lazy_frame.collect() {
                Ok(df) => {
                    self.dataframe = Some(df);
                    if let Some(dataframe) = &self.dataframe {
                        self.column_names = dataframe
                            .get_column_names()
                            .iter()
                            .map(|s| s.to_string())
                            .collect();
                    }
                }
                Err(e) => {
                    self.error_message = Some(format!("Error collecting DataFrame: {}", e));
                }
            },
            Err(e) => {
                self.error_message = Some(format!("Error scanning Parquet files: {}", e));
            }
        }
    }
}

impl eframe::App for Tablr {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if self.files_to_load.len() != 0 {
            self.load_parquet_data(&self.files_to_load.clone());
            self.files_to_load.clear();
        }

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("Browse...").clicked() {
                    if let Some(paths) = FileDialog::new()
                        .add_filter("Parquet files", &["parquet"])
                        .set_title("Pick parquet file(s)")
                        .pick_files()
                    {
                        self.files_message = if paths.len() == 1 {
                            paths[0]
                                .file_name()
                                .unwrap_or_default()
                                .to_string_lossy()
                                .to_string()
                        } else {
                            format!("{} files", paths.len())
                        };

                        if !paths.is_empty() {
                            self.files_to_load = paths;
                        } else {
                            self.dataframe = None;
                            self.column_names.clear();
                            self.error_message = Some("Please select a file path.".to_string());
                        }
                    }
                }

                if self.files_message.is_empty() {
                    ui.label("No parquet file selected.");
                } else {
                    ui.label(format!("Loaded file: {}", self.files_message));
                }
            });

            ui.separator();

            if let Some(err_msg) = &self.error_message {
                ui.colored_label(egui::Color32::RED, err_msg);
            }

            if let Some(df) = &self.dataframe {
                if !self.column_names.is_empty() {
                    ScrollArea::horizontal()
                        .auto_shrink([false, false])
                        .show(ui, |ui| {
                            TableBuilder::new(ui)
                                .striped(true)
                                .resizable(true)
                                .columns(Column::auto().resizable(true), self.column_names.len())
                                .header(20.0, |mut header| {
                                    header.col(|ui| {
                                        ui.label("Row Index");
                                    });
                                    for col_name in &self.column_names {
                                        header.col(|ui| {
                                            ui.add(
                                                Label::new(col_name)
                                                    .wrap_mode(TextWrapMode::Extend),
                                            );
                                        });
                                    }
                                })
                                .body(|body| {
                                    let num_rows = df.height();
                                    body.rows(20.0, num_rows, |mut row| {
                                        let row_index = row.index();

                                        row.col(|ui| {
                                            ui.label(row_index.to_string());
                                        });

                                        for col_name in &self.column_names {
                                            match df.column(col_name) {
                                                Ok(series) => {
                                                    let cell_text = match series.get(row_index) {
                                                        Ok(any_value) => any_value.to_string(),
                                                        Err(_) => "Error (cell access)".to_string(),
                                                    };
                                                    row.col(|ui| {
                                                        ui.add(
                                                            Label::new(cell_text)
                                                                .wrap_mode(TextWrapMode::Extend),
                                                        );
                                                    });
                                                }
                                                Err(_) => {
                                                    row.col(|ui| {
                                                        ui.add(
                                                            Label::new("Error (column access)")
                                                                .wrap_mode(TextWrapMode::Extend),
                                                        );
                                                    });
                                                }
                                            }
                                        }
                                    });
                                });
                        });
                } else if self.error_message.is_none() {
                    ui.label("File loaded, but it contains no columns or data.");
                }
            }
        });
    }
}

fn main() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Tablr - Parquet Viewer",
        options,
        Box::new(|cc| Ok(Box::new(Tablr::new(cc)))),
    )
}
