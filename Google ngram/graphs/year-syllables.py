import plotly.plotly as py
import os
py.sign_in('DemoAccount', 'lr1c37zw81')
import plotly.graph_objs as go

cwd = os.getcwd()
cwd = cwd.replace('graphs', '')
file = open(cwd + "output/third", "r+")
year = []
wordCount = []
for line in iter(file):
	temp = line.split()
	year.append(temp[0])
	wordCount.append(temp[1])
	
data = [
    go.Scatter(
        x=year,
        y=wordCount,
        marker=dict(
            color='rgb(171, 32, 73)',
        ),
        opacity=0.6
    )
]
plot_url = py.plot(data, filename='year-wordcount')
print plot_url
