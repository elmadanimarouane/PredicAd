package api

import java.text.SimpleDateFormat


object PredicadApi {

  // This method allows us to convert a timestamp into an hour
  def timestampToHour: String => Double = (timestamp: String) =>
    {
      // We convert our string to a long
      val newTimeStamp = timestamp.toInt * 1000L
      // We initialize a DateFormat based on the hour
      val dateFormat = new SimpleDateFormat("HH")
      // We return our timestamp into a hour (int type)
      dateFormat.format(newTimeStamp).toDouble
    }

  // This method allows us to remove anything after the "-" in the interests column
  def handleInterests: String => String = (interest: String) =>
    {
      Option(interest) match
        {
        case Some(x) =>
          if(x contains "-")
          {
            x.substring(0,x.indexOf("-"))
          }
          else
          {
            x
          }
        case None => "Unknown"
      }
    }

  // This method allows us to convert code network to french operators
  def handleNetwork: String => String = (networkCode: String) =>
    {
      // We convert our string into an option to handle null values
      Option(networkCode) match
        {
        case Some(x) =>
        // We first make sure to get all the french operators
          if(x.contains("208-"))
          {
            // We get orange's codes
            if(x.contains("-01") || x.contains("-1") || x.contains("-02") || x.contains("-2"))
            {
              "Orange"
            }
            // SFR's codes
            else if(x.contains("-09") || x.contains("-9") || x.contains("-10") || x.contains("-11")
              || x.contains("-13"))
            {
              "SFR"
            }
            else if(x.contains("-15") || x.contains("-16"))
            {
              "Free"
            }
            // Bouygues Telecom's codes
            else if(x.contains("-20") || x.contains("-21") || x.contains("-01"))
            {
              "Bouygues Telecom"
            }
            else
            {
              "Unknown"
            }
          }
          else
          {
            "Unknown"
          }
        case None => "Unknown"
      }
    }

  // This method allows us to correct the name of the os (for example, "android" into "Android")
  def correctOS: String => String = (osName: String) =>
    {
      Option(osName) match
        {
        case Some(x) =>
        if(x == "android")
        {
          "Android"
        }
        else if(x == "ios")
        {
          "iOS"
        }
        else if(x == "Windows Phone OS" || x == "WindowsPhone")
        {
          "Windows Phone"
        }
        else
        {
          "Other"
        }
        case None => "Unknown"
      }
    }

  // This method allows us to transform our boolean from our "label" column into 1 or 0. It will be used later for our
  // logistic regression algorithm
  def convertLabel: Boolean => Double = (labelValue: Boolean) => if(labelValue) 1.0 else 0.0

  // This method allows us to convert our array of size into a simple string like "320x480"
  def convertSize: Seq[String] => String = (size: Seq[String]) =>
    Option(size) match
    {
      case Some(x) => x.head + "x" + x.tail.head
      case None => "Unknown"
    }

}
