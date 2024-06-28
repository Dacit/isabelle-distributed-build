source("setup.R")

data <- data.frame(read.csv(file = "data/compare_pth_params.csv", head = TRUE))

map_splits <- function(name) {
  if (name == "fixed fraction (0.1)") c(1, "0.1") else
  if (name == "fixed fraction (0.3)") c(2, "0.3") else
  if (name == "fixed fraction (0.5)") c(3, "Fixed: 0.5") else
  if (name == "host speed (0.5)") c(4, "0.5") else
  if (name == "host speed (0.7)") c(5, "0.7") else
  if (name == "host speed (0.9)") c(6, "Speed: 0.9") else
  if (name == "per critical node") c(7, "Max Critical") else
    c(8, "")
}

data_cont <- data %>% rowwise() %>% mutate(
  i_parallel_threads = factor(parallel_threads),
  i_is_criticals = factor(is_criticals, levels = c(
    "absolute time (0:01:00)",
    "absolute time (0:10:00)",
    "absolute time (0:20:00)",
    "absolute time (0:30:00)",
    "relative time (0.25)",
    "relative time (0.5)",
    "relative time (0.75)",
    "relative time (0.9)"), labels = c(
    "Absolute: \\SI{1}{\\minute}",
    "\\SI{10}{\\minute}",
    "\\SI{20}{\\minute}",
    "\\SI{30}{\\minute}",
    "Relative: 0.25",
    "0.50",
    "0.75",
    "0.90")),
  i_machine_splits = factor(machine_splits, levels = c(
    "fixed fraction (0.1)",
    "fixed fraction (0.5)",
    "host speed (0.5)",
    "host speed (0.9)",
    "per critical node"), labels = c(
    "0.1",
    "Fixed: 0.5",
    "0.5",
    "Speed: 0.9",
    "Max Critical"))) %>%
  filter(!is.na(i_machine_splits)) %>% filter(!is.na(i_is_criticals))

ggplot(data_cont, aes(y = i_machine_splits, x = i_is_criticals, fill = timing / 60)) +
  geom_tile() +
  labs(x = "Is Critical", y = "Machine Splits", fill = "Make-\nspan [h]") +
  facet_wrap(~parallel_threads, ncol = 3) +
  scale_fill_gradientn(colors = c("red", "orange", "transparent"), values = c(0, 0.25, 1)) +
  theme_pubr(legend = "right") +
  theme(axis.text.x = element_text(angle = 90, hjust = 1, vjust = 0.5))