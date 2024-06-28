source("setup.R")

data0 <- data.frame(read.csv(file="data/host_factors_error.csv", head=TRUE))
data <- data0 %>% filter(time < 240 & error < 0.2) %>% mutate(other = "s")

good_time <- 60
good_score <- 0.015

ggplot(data, aes(x=error, y=time)) +
  geom_point(data=subset(data, time <= good_time & error <= good_score), aes(shape=session, color=session)) +
  geom_point(data=subset(data, time > good_time | error > good_score), aes(shape=other, color=other)) +
  labs(x = "Mean Squared Error", y = "Cumulative Benchmark Time [\\si{\\second}]", color = "Session", shape = "Session") +
  geom_label_repel(data=subset(data, time <= good_time & error <= good_score), aes(label=session),
                   nudge_y = 60, nudge_x = 0.052) +
  scale_y_continuous(breaks = c(0, 30, 60, 90, 120, 150, 180, 210, 240)) +
  theme_pubr() +
  scale_color_manual(values = pubr_palette(6)) +
  theme(legend.position = "none")