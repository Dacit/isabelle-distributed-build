source("setup.R")

data_cplex_5 <- data.frame(read.csv(file="data/schedule_analysis_solve_fast_2_AFP.csv", head=TRUE))
data_heuristic <- data.frame(read.csv(file="data/schedule_analysis_heuristic_2_AFP.csv", head=TRUE))

data <- bind_rows(
  data_cplex_5 %>% mutate(schedule = "CPLEX \\SI{5}{\\minute}"),
  data_heuristic %>% mutate(schedule = "Heuristic")) %>%
  mutate(expected_end = expected_start + expected_duration, actual_end = actual_start + actual_duration) %>%
  mutate(delay = (actual_start - expected_start) / 60, deviation = (actual_duration - expected_duration) / 60)

mean((data %>% filter(schedule == "Heuristic"))$delay)
sum((data %>% filter(schedule == "Heuristic"))$deviation)
mean((data %>% filter(schedule == "CPLEX \\SI{5}{\\minute}"))$delay)
sum((data %>% filter(schedule == "CPLEX \\SI{5}{\\minute}"))$deviation)

ggplot(data, aes(y = delay, x = deviation, color = schedule)) +
  geom_point(aes(shape = schedule)) +
  geom_ysidehistogram(size = 0.2, binwidth =  0.8, alpha = 0.4, aes(fill = schedule), show.legend = FALSE, position = "identity") +
  geom_xsidehistogram(size = 0.2, binwidth = 0.2, alpha = 0.4, aes(fill = schedule), show.legend = FALSE, position = "identity") +
  labs(y = "Accumulated Delay [\\si{\\min}]", x = "Session Run-time Deviation [\\si{\\min}]", color = "", shape = "") +
  scale_shape_manual(values=c(1, 2)) +
  scale_y_continuous(breaks = c(-5, 0, 5, 10, 15)) +
  scale_x_continuous(breaks = c(-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5)) +
  scale_ysidex_continuous(labels = NULL, breaks = NULL) +
  scale_xsidey_continuous(labels = NULL, breaks = NULL) +
  scale_color_manual(values = c(
    pubr_palette(6)[6],
    pubr_palette(4)[4]
  )) +
  scale_fill_manual(values = c(
    pubr_palette(6)[6],
    pubr_palette(4)[4]
  )) +
  theme_pubr()