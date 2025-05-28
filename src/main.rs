use eframe::egui::{
    self, CentralPanel, Color32, ComboBox, Context, CursorIcon, RichText, TextStyle, Ui,
    ViewportBuilder, Window,
};
use eframe::egui::{ScrollArea, TextWrapMode};
use egui::widgets::Label;
use egui_extras::{Column, TableBody, TableBuilder, TableRow};
use polars::prelude::*;
use rfd::FileDialog;
use std::env;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq)]
enum FilterType {
    Equals,
    Contains,
}

impl std::fmt::Display for FilterType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterType::Equals => write!(f, "Equals"),
            FilterType::Contains => write!(f, "Contains"),
        }
    }
}

struct Tablr {
    dataframe: Option<DataFrame>,
    original_dataframe: Option<DataFrame>,
    column_names: Vec<String>,
    files_to_load: Vec<PathBuf>,
    error_message: Option<String>,
    files_loaded: bool,

    sort_column: Option<usize>,
    sort_descending: bool,

    filter_dialog_open: bool,
    selected_filter_column: Option<usize>,
    filter_text: String,
    filter_type: FilterType,
}

impl Tablr {
    fn new(files_to_load: Vec<PathBuf>) -> Self {
        Self {
            dataframe: None,
            original_dataframe: None,
            column_names: Vec::new(),
            files_to_load,
            error_message: None,
            files_loaded: false,

            sort_column: None,
            sort_descending: false,

            filter_dialog_open: false,
            selected_filter_column: None,
            filter_text: String::new(),
            filter_type: FilterType::Equals,
        }
    }

    fn load_parquet_data(&mut self, paths: Vec<PathBuf>) {
        self.dataframe = None;
        self.original_dataframe = None;
        self.column_names.clear();

        let scan_sources = ScanSources::Paths(paths.into());

        match LazyFrame::scan_parquet_sources(scan_sources, ScanArgsParquet::default())
            .and_then(|lazy_frame| lazy_frame.collect())
        {
            Ok(df) => {
                let df_with_row_index = df.with_row_index("Row Index".into(), None).unwrap();
                self.column_names = df_with_row_index
                    .get_column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                self.original_dataframe = Some(df_with_row_index.clone());
                self.dataframe = Some(df_with_row_index);
                self.error_message = None;
                self.selected_filter_column = None;
                self.filter_text.clear();
            }
            Err(e) => {
                self.dataframe = None;
                self.original_dataframe = None;
                self.column_names.clear();
                self.error_message = Some(format!("Error processing Parquet files: {}", e));
            }
        }
    }

    fn process_pending_files(&mut self) {
        if !self.files_loaded && !self.files_to_load.is_empty() {
            self.load_parquet_data(self.files_to_load.clone());
            self.files_loaded = true;
        }
    }

    fn render_file_selector(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            if ui.button("Browse...").clicked() {
                self.handle_browse_button_click();
            }

            if self.files_to_load.is_empty() {
                ui.label("No parquet files selected");
            } else if self.files_to_load.len() == 1 {
                ui.label(format!(
                    "Selected: {}",
                    self.files_to_load[0].file_name().unwrap().to_string_lossy()
                ));
            } else {
                ui.label(format!("Selected: {} files", self.files_to_load.len()));
            }

            ui.separator();

            ui.add_enabled_ui(self.dataframe.is_some(), |ui| {
                if ui.button("Filter").clicked() {
                    self.filter_dialog_open = true;
                }
            });
        });
    }

    fn handle_browse_button_click(&mut self) {
        if let Some(paths) = FileDialog::new()
            .add_filter("Parquet files", &["parquet"])
            .pick_files()
        {
            if paths.is_empty() {
                self.error_message =
                    Some("No files selected. Please select at least one Parquet file.".to_string());
            } else {
                self.files_to_load = paths;
                self.files_loaded = false;
                self.error_message = None;
            }
        }
    }

    fn render_error_message(&self, ui: &mut Ui) {
        if let Some(err_msg) = &self.error_message {
            ui.colored_label(Color32::RED, err_msg);
        }
    }

    fn render_dataframe(&mut self, ui: &mut Ui) {
        if let Some(df) = &self.dataframe.clone() {
            ScrollArea::horizontal()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    TableBuilder::new(ui)
                        .striped(true)
                        .resizable(true)
                        .columns(Column::auto().resizable(true), self.column_names.len() + 1)
                        .header(20.0, |mut header_row| {
                            self.render_table_header(&mut header_row, &self.column_names.clone());
                        })
                        .body(|body| {
                            self.render_table_body(body, df, &self.column_names);
                        });
                });
        }
    }

    fn apply_sort(&mut self) {
        if let (Some(df), Some(col_idx)) = (&self.dataframe, self.sort_column) {
            let col_name = &self.column_names[col_idx];

            match df.sort(
                vec![PlSmallStr::from(col_name)],
                SortMultipleOptions::new().with_order_descending(self.sort_descending),
            ) {
                Ok(sorted_df) => {
                    self.dataframe = Some(sorted_df);
                }
                Err(e) => {
                    self.error_message = Some(format!("Sort error: {}", e));
                }
            }
        }
    }

    fn apply_filter(&mut self) {
        if let (Some(original_df), Some(col_idx)) =
            (&self.original_dataframe, self.selected_filter_column)
        {
            let col_name = &self.column_names[col_idx];

            if self.filter_text.is_empty() {
                self.dataframe = Some(original_df.clone());
            } else {
                let lazy_df = original_df.clone().lazy();
                let filter_expr = match self.filter_type {
                    FilterType::Equals => col(col_name).eq(lit(self.filter_text.clone())),
                    FilterType::Contains => col(col_name)
                        .str()
                        .contains(lit(self.filter_text.clone()), false),
                };
                match lazy_df.filter(filter_expr).collect() {
                    Ok(filtered_df) => {
                        self.dataframe = Some(filtered_df);
                    }
                    Err(e) => {
                        self.error_message = Some(format!("Filter error: {}", e));
                        self.dataframe = Some(original_df.clone());
                    }
                }
            }

            if self.sort_column.is_some() {
                self.apply_sort();
            }
        }
    }

    fn render_filter_dialog(&mut self, ctx: &Context) {
        let mut open = self.filter_dialog_open;
        Window::new("Filter")
            .resizable(false)
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ComboBox::from_id_salt("filter_column")
                        .selected_text(
                            self.selected_filter_column
                                .map(|idx| self.column_names[idx].as_str())
                                .unwrap_or("Select column"),
                        )
                        .show_ui(ui, |ui| {
                            let column_names = self.column_names.clone();
                            for (idx, col_name) in column_names.iter().enumerate() {
                                if ui
                                    .selectable_value(
                                        &mut self.selected_filter_column,
                                        Some(idx),
                                        col_name,
                                    )
                                    .clicked()
                                {
                                    self.apply_filter();
                                }
                            }
                        });

                    ui.add_enabled_ui(self.selected_filter_column.is_some(), |ui| {
                        ComboBox::from_id_salt("filter_type")
                            .selected_text(self.filter_type.to_string())
                            .show_ui(ui, |ui| {
                                if ui
                                    .selectable_value(
                                        &mut self.filter_type,
                                        FilterType::Equals,
                                        FilterType::Equals.to_string(),
                                    )
                                    .clicked()
                                {
                                    self.apply_filter();
                                }
                                if ui
                                    .selectable_value(
                                        &mut self.filter_type,
                                        FilterType::Contains,
                                        FilterType::Contains.to_string(),
                                    )
                                    .clicked()
                                {
                                    self.apply_filter();
                                }
                            });

                        ui.label("Filter text:");
                        let response = ui.text_edit_singleline(&mut self.filter_text);
                        if response.changed() {
                            self.apply_filter();
                        }
                        if ui.button("Clear Filter").clicked() {
                            self.selected_filter_column = None;
                            self.filter_text.clear();
                            if let Some(original_df) = &self.original_dataframe {
                                self.dataframe = Some(original_df.clone());
                                if self.sort_column.is_some() {
                                    self.apply_sort();
                                }
                            }
                        }
                    });
                });
            });
        self.filter_dialog_open = open;
    }

    fn render_table_header(&mut self, header_row: &mut TableRow, column_names: &[String]) {
        for (i, col_name) in column_names.iter().enumerate() {
            header_row.col(|ui| {
                if ui
                    .add(
                        Label::new(
                            RichText::new(&format!(
                                "{} {}",
                                col_name,
                                if Some(i) == self.sort_column {
                                    if self.sort_descending { "⬇" } else { "⬆" }
                                } else {
                                    ""
                                }
                            ))
                            .strong(),
                        )
                        .wrap_mode(TextWrapMode::Extend),
                    )
                    .on_hover_cursor(CursorIcon::Default)
                    .clicked()
                {
                    if Some(i) == self.sort_column {
                        self.sort_descending = !self.sort_descending;
                    } else {
                        self.sort_column = Some(i);
                        self.sort_descending = false;
                    }
                    self.apply_sort();
                }
            });
        }
    }

    fn render_table_body(&self, body: TableBody, df: &DataFrame, column_names: &[String]) {
        let num_rows = df.height();
        body.rows(20.0, num_rows, |mut row| {
            for col_name in column_names {
                match df.column(col_name) {
                    Ok(column) => {
                        let cell_text = match column.get(row.index()) {
                            Ok(any_value) => any_value.to_string(),
                            Err(_) => "Error".to_string(),
                        };
                        row.col(|ui| {
                            ui.add(Label::new(&cell_text).wrap_mode(TextWrapMode::Extend));
                        });
                    }
                    Err(_) => {
                        row.col(|ui| {
                            ui.add(Label::new("Col?").wrap_mode(TextWrapMode::Extend));
                        });
                    }
                }
            }
        });
    }
}

impl eframe::App for Tablr {
    fn update(&mut self, ctx: &Context, _frame: &mut eframe::Frame) {
        let font_size = 18.;
        ctx.style_mut(|style| {
            style.text_styles.get_mut(&TextStyle::Body).unwrap().size = font_size;
            style.text_styles.get_mut(&TextStyle::Button).unwrap().size = font_size;
        });

        self.process_pending_files();
        self.render_filter_dialog(ctx);
        CentralPanel::default().show(ctx, |ui| {
            self.render_file_selector(ui);
            ui.separator();
            self.render_error_message(ui);
            self.render_dataframe(ui);
        });
    }
}

fn main() -> Result<(), eframe::Error> {
    let paths: Vec<PathBuf> = env::args().skip(1).map(PathBuf::from).collect();

    let options = eframe::NativeOptions {
        viewport: ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Tablr - Parquet Viewer",
        options,
        Box::new(|_| Ok(Box::new(Tablr::new(paths)))),
    )
}
