require "spec"
require "../src/crysda"

module Crysda
  SLEEP_DATA = read_csv("./spec/data/msleep.csv")
  IRIS_DATA  = read_csv("./spec/data/iris.txt", '\t')
end
