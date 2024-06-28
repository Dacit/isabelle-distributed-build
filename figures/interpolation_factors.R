source("setup.R")

data <- data.frame(read.csv(file="data/interpolation_factors.csv", head=TRUE))

ggplot(data, aes(x=factor(kind, levels=c(
  "Interpolate Global Curve",
  "Interpolate Individually",
  "Interpolate",
  "Scale Up"
)), y=error)) +
  geom_boxplot(color=pubr_palette(2)[2], outlier.shape = 1) +
  stat_boxplot(geom = 'errorbar', width = 0.2, color=pubr_palette(2)[2]) +
  coord_flip() +
  ylim(c(0, 3))+
  ylab("Mean Relative Error") +
  xlab("Scenario") +
  theme_pubr()