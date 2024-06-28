source("setup.R")

data_bootstrap <- data.frame(read.csv(file = "data/bootstrap.csv", head = TRUE)) %>%
  mutate(initial = as.numeric(hms(initial)), timing = as.numeric(hms(timing)), start = as.numeric(hms(start)))

data <-
  bind_rows(
    data_bootstrap %>% transmute(run = run, time = timing - start, kind = "Actual"),
    data_bootstrap %>% transmute(run = run, time = initial, kind = "Estimate")) %>%
  mutate(time = time / 3600, kind = factor(kind, levels = c("Estimate", "Actual"))) %>%
  filter(run <= 12)

ggplot(data, aes(x = run)) +
  geom_point(aes(y = time, color = kind, shape = kind)) +
  geom_line(aes(y = time, color = kind), linewidth = 0.25, alpha = 0.25) +
  labs(x = "Number of Previous Runs", y = "Wall-clock Time [\\si{\\hour}]", color = "", shape = "") +
  scale_y_continuous(breaks = c(0.5, 1, 2, 4, 8, 16, 32), trans = "log2") +
  scale_x_continuous(breaks = c(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) +
  theme_pubr() +
  scale_color_manual(values = c(pubr_palette(4)[4], pubr_palette(5)[5])) +
  theme(legend.position = "right")