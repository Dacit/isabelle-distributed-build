suppressPackageStartupMessages(library(ppcor))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(tibble))
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(ggpubr))
suppressPackageStartupMessages(library(extrafont))
suppressPackageStartupMessages(library(tikzDevice))
suppressPackageStartupMessages(library(tinytex))
suppressPackageStartupMessages(library(ggrepel))
suppressPackageStartupMessages(library(reshape2))
suppressPackageStartupMessages(library(ggbreak))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(ggside))

set.seed(42)

pubr_palette <- function(n) {
  colors <- c(
    rgb(245, 105, 27, maxColorValue = 255), # red / orange
    rgb(119, 39, 27, maxColorValue = 255), # dark brown
    rgb(105, 208, 134, maxColorValue = 255), # green
    rgb(67, 156, 255, maxColorValue = 255), # light blue / turquoise
    rgb(97, 75, 202, maxColorValue = 255), # dark blue / purple
    rgb(199, 42, 119, maxColorValue = 255)) # violet

  structure(rep(colors, length.out = n), name = "pubr", class = "palette")
}

theme_pubr <- function(legend = c("top", "bottom", "left", "right", "none")) {
  base_size <- 10
  half_line <- base_size / 2
  if (!is.numeric(legend)) legend <- match.arg(legend)
  panel.border <- element_blank()
  axis.line <- element_line(colour = "black", size = 0.5)
  plot.margin <- margin(0, half_line, half_line, half_line)
  .theme <- theme_bw(base_size = base_size) %+replace% theme(panel.border = panel.border, panel.grid.major = element_blank(), panel.grid.minor = element_blank(), axis.line = axis.line, axis.text = element_text(color = "black"), legend.key = element_blank(), strip.background = element_rect(colour = "black"), plot.margin = plot.margin, legend.position = legend, complete = TRUE)
  .theme <- .theme + theme(axis.title.x = element_text(margin = margin(t = half_line)), axis.title.y = element_text(margin = margin(r = half_line)))
  .theme
}
