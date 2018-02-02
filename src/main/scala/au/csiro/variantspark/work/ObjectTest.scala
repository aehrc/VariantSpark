package au.csiro.variantspark.work


object Parent {
  def method() {
    println("Parent")
  }
  
  
  def indirect() {
    method()
  }
  
  def apply() {
    indirect()
  } 
}


object Child {
  
  import Parent._
  
  def method() {
    print("Child")
  }
  
  def apply() {
    indirect()
  }
}

object ObjectTest {
  
  def main(argv:Array[String]) {
    println("Hello")
    
    Parent()
    Child()
  }
}