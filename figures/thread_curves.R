source("setup.R")

data0 <- data.frame(read.csv(file="data/thread_curves.csv", head=TRUE))
data <- data0 %>% mutate(shape = job %% 7)

ggplot(data, aes(x=threads, y=factor, color=as.factor(job))) +
  geom_line(linewidth = 0.25, alpha = 0.5) +
  geom_point(shape = data$shape) +
  labs(y = "Speedup Factor", x = "Number of Threads") +
  scale_x_continuous(trans = "log2", breaks = c(1, 2, 4, 8, 16, 32, 64, 128)) +
  scale_y_continuous(breaks = c(1, 2, 3, 4, 5, 6)) +
  theme_pubr() +
  scale_color_manual(values = pubr_palette(43)) +
  theme(legend.position = "none")