source("setup.R")

data_compare_schedules <- data.frame(read.csv(file = "data/compare_schedules.csv", head = TRUE))

data <- data_compare_schedules %>%
  mutate(
    value = result / 3600,
    method = factor(method, levels = c(
      "heuristic",
      "cplex-5",
      "cplex-180",
      "bound"
    ), labels = c(
      "Heuristic",
      "CPLEX \\SI{5}{\\minute}",
      "CPLEX \\SI{3}{\\hour}",
      "Lower Bound"
    )),
    config = factor(config, levels = c(
      "4_Full",
      "3_Slow",
      "2_AFP",
      "1_Dist"
    ), labels = c(
      "Full AFP",
      "Slow AFP",
      "AFP",
      "Distribution"
    )),
    dataset = factor(dataset, levels = c("B", "B1", "C", "C2"),
      labels = c(
        "Heterogeneous",
        "Heterogeneous\n(4/14 machines)",
        "Release",
        "Release\n(4/8 machines)"))) %>%
  filter(!is.na(config)) %>% filter(!is.na(value))

ggplot(data, aes(x = config, y = value, fill = method)) +
  facet_wrap(~dataset, ncol = 4) +
  coord_flip() +
  geom_bar(position = "dodge", stat = "identity") +
  labs(x = NULL, y = "Makespan [\\si{\\hour}]", fill = "Method") +
  scale_y_continuous(breaks = c(0, 1, 2, 3, 4, 5)) +
  theme_pubr() +
  scale_fill_manual(values = c(
    pubr_palette(4)[4],
    pubr_palette(6)[6],
    pubr_palette(1)[1],
    pubr_palette(3)[3]
  )) +
  guides(fill = guide_legend(reverse = TRUE))