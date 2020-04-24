from tkinter import *
from tkinter.tix import Tk, Control, ComboBox  #升级的组合控件包
from tkinter.messagebox import showinfo, showwarning, showerror #各种类型的提示框

root = Tk() # 初始化Tk()
root.title("hello tkinter")    # 设置窗口标题
root.geometry("800x1000")    # 设置窗口大小 注意：是x 不是*
root.resizable(width=True, height=True) # 设置窗口是否可以变化长/宽，False不可变，True可变，默认为True
root.tk.eval('package require Tix')  #引入升级包，这样才能使用升级的组合控件

lable = Label(root, text="label", bg="pink",bd=10, font=("Arial",12), width=8, height=3)
lable.pack(side=LEFT)

# Booleans
NO = FALSE = OFF = 0
YES = TRUE = ON = 1

# -anchor and -sticky
N = 'n'
S = 's'
W = 'w'
E = 'e'
NW = 'nw'
SW = 'sw'
NE = 'ne'
SE = 'se'
NS = 'ns'
EW = 'ew'
NSEW = 'nsew'
CENTER = 'center'

# -fill
NONE = 'none'
X = 'x'
Y = 'y'
BOTH = 'both'

# -side
LEFT = 'left'
TOP = 'top'
RIGHT = 'right'
BOTTOM = 'bottom'

# -relief
RAISED = 'raised'
SUNKEN = 'sunken'
FLAT = 'flat'
RIDGE = 'ridge'
GROOVE = 'groove'
SOLID = 'solid'

# -orient
HORIZONTAL = 'horizontal'
VERTICAL = 'vertical'

# -tabs
NUMERIC = 'numeric'

# -wrap
CHAR = 'char'
WORD = 'word'

# -align
BASELINE = 'baseline'

# -bordermode
INSIDE = 'inside'
OUTSIDE = 'outside'

# Special tags, marks and insert positions
SEL = 'sel'
SEL_FIRST = 'sel.first'
SEL_LAST = 'sel.last'
END = 'end'
INSERT = 'insert'
CURRENT = 'current'
ANCHOR = 'anchor'
ALL = 'all'  # e.g. Canvas.delete(ALL)

# Text widget and button states
NORMAL = 'normal'
DISABLED = 'disabled'
ACTIVE = 'active'
# Canvas state
HIDDEN = 'hidden'

# Menu item types
CASCADE = 'cascade'
CHECKBUTTON = 'checkbutton'
COMMAND = 'command'
RADIOBUTTON = 'radiobutton'
SEPARATOR = 'separator'

# Selection modes for list boxes
SINGLE = 'single'
BROWSE = 'browse'
MULTIPLE = 'multiple'
EXTENDED = 'extended'

# Activestyle for list boxes
# NONE='none' is also valid
DOTBOX = 'dotbox'
UNDERLINE = 'underline'

# Various canvas styles
PIESLICE = 'pieslice'
CHORD = 'chord'
ARC = 'arc'
FIRST = 'first'
LAST = 'last'
BUTT = 'butt'
PROJECTING = 'projecting'
ROUND = 'round'
BEVEL = 'bevel'
MITER = 'miter'

# Arguments to xview/yview
MOVETO = 'moveto'
SCROLL = 'scroll'
UNITS = 'units'
PAGES = 'pages'



frame2=Frame(root)
frame2.pack(fill=X)
lable2=Label(frame2,text='您的性别：  ')
lable2.grid(row=1,column=0)
sex=StringVar()
sex_male=Radiobutton(frame2,text='男',fg='blue',variable=sex,value='男')
sex_male.grid(row=1,column=2)
sex_female=Radiobutton(frame2,text='女',fg='red',variable=sex,value='女')
sex_female.grid(row=1,column=4)


frame4 =Frame(root)
frame4.pack(fill=X)
lable4=Label(frame4,text='4、请删除您不会的变成语言：  ')
lable4.grid(row=1,column=0)
listbox=Listbox(frame4)
listbox.grid(row=1,column=1)
for item in ["C","C++","JAVA","PYTHON","R","SQL","JS"]:
    listbox.insert(END,item)
DELETE=Button(frame4,text="删除",command=lambda listbox=listbox:listbox.delete(ANCHOR))
DELETE.grid(row=1,column=2)
language=Button(frame4,text="确定")
language.grid(row=2,column=1)

from tkinter import *







frame8=Frame(root)
frame8.pack()
agree=StringVar()
agree=Checkbutton(frame8,text='我同意',variable=agree,onvalue='确定',offvalue="不确定",)
agree.grid()


frame10=Frame(root)
frame10.pack()
group=LabelFrame(frame10,text='特别鸣谢',padx=5,pady=5)
group.grid()
w=Label(group,text='容器框')
w.pack()




button=Button(root,text='QUIT',command=root.quit,activeforeground="black",activebackground='blue',bg='red',fg='white')
button.pack(fill=Y,expand=1)



























root.mainloop()